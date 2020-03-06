using ParquetFileViewer.Utilities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ParquetFileViewer
{
    public class ParquetFlatReader : IDisposable
    {
        private Task<long> recordCountTask = null;
        private List<Parquet.ParquetReader> readers = new List<Parquet.ParquetReader>();

        public ParquetFlatReader(string filePath, int degreeOfParallelism = 1, Parquet.ParquetOptions parquetOptions = null)
        {
            if (!File.Exists(filePath))
                throw new FileNotFoundException(filePath);

            if (degreeOfParallelism <= 0)
                throw new ArgumentException("degreeOfParallelism must be a positive integer");

            this.recordCountTask = Task.Run(() =>
            {
                using (var benchmark = new SimpleBenchmark("GetRowCountAsync"))
                {
                    using (var rowCountReader = new Parquet.ParquetReader(new FileStream(filePath, FileMode.Open, FileAccess.Read),
                        parquetOptions,
                        false))
                    {
                        long count = 0;
                        for (int i = 0; i < rowCountReader.RowGroupCount; i++)
                        {
                            using (var groupReader = rowCountReader.OpenRowGroupReader(i))
                            {
                                count += groupReader.RowCount;
                            }
                        }

                        return count;
                    }
                }
            });

            for (int i = 0; i < degreeOfParallelism; i++)
            {
                this.readers.Add(
                    new Parquet.ParquetReader(new FileStream(filePath, FileMode.Open, FileAccess.Read),
                        parquetOptions,
                        false)
                    );
            }
        }

        public Task<long> GetRowCount()
        {
            return this.recordCountTask;
        }

        public async Task<DataTable> ToDataTable(List<string> selectedFields, long offset, long recordCount)
        {
            using (var benchmark = new SimpleBenchmark("ToDataTable"))
            {
                int concurrency = Math.Min(this.readers.Count, selectedFields.Count);
                int fieldsPerThread = selectedFields.Count / concurrency;

                int threadIndex = 0;
                var tasks = new List<Task<DataTable>>();
                foreach (List<string> fields in UtilityMethods.SplitList(selectedFields, fieldsPerThread))
                {
                    tasks.Add(this.ToDataTableInternal(this.readers[threadIndex], fields, offset, recordCount));
                    threadIndex++;
                }

                DataTable finalResult = new DataTable();
                while (tasks.Count > 0)
                {
                    var completedTask = await Task.WhenAny(tasks);
                    finalResult = UtilityMethods.MergeTablesByIndex(finalResult, completedTask.Result);
                    tasks.Remove(completedTask);
                }

                return finalResult; //TODO: Test column order with large file
                                    //return finalResult.SetColumnsOrder(selectedFields);
            }
        }

        private async Task<DataTable> ToDataTableInternal(Parquet.ParquetReader reader, List<string> selectedFields, long offset, long recordCount)
        {
            using (var benchmark = new SimpleBenchmark("ToDataTableInternal"))
            {
                //Get list of data fields and construct the DataTable
                DataTable dataTable = new DataTable();
                List<Parquet.Data.DataField> fields = new List<Parquet.Data.DataField>();
                var dataFields = reader.Schema.GetDataFields();
                foreach (string selectedField in selectedFields)
                {
                    var dataField = dataFields.FirstOrDefault(f => f.Name.Equals(selectedField, StringComparison.InvariantCultureIgnoreCase));
                    if (dataField != null)
                    {
                        fields.Add(dataField);
                        DataColumn newColumn = new DataColumn(dataField.Name, ParquetNetTypeToCSharpType(dataField.DataType));
                        dataTable.Columns.Add(newColumn);
                    }
                    else
                        throw new Exception(string.Format("Field '{0}' does not exist", selectedField));
                }

                long totalRecordCount = 0;
                long rowsLeftToRead = recordCount;

                //Read column by column to generate each row in the datatable
                for (int i = 0; i < reader.RowGroupCount; i++)
                {
                    bool readGroup = false;

                    long rowsPassedUntilThisRowGroup = -1;
                    long groupRowCount = -1;
                    using (var groupReader = reader.OpenRowGroupReader(i))
                    {
                        rowsPassedUntilThisRowGroup = totalRecordCount;
                        totalRecordCount += (int)groupReader.RowCount;

                        if (offset >= totalRecordCount)
                            continue;

                        if (rowsLeftToRead > 0)
                            readGroup = true;
                        else
                            break;

                        groupRowCount = groupReader.RowCount;

                        if (readGroup)
                        {
                            long numberOfRecordsToReadFromThisRowGroup = Math.Min(Math.Min(totalRecordCount - offset, rowsLeftToRead), groupRowCount);
                            rowsLeftToRead -= numberOfRecordsToReadFromThisRowGroup;

                            long recordsToSkipInThisRowGroup = Math.Max(offset - rowsPassedUntilThisRowGroup, 0);

                            await Task.Run(() =>
                            {
                                ProcessRowGroup(dataTable, groupReader, fields, recordsToSkipInThisRowGroup, numberOfRecordsToReadFromThisRowGroup);
                            });
                        }
                    }
                }

                return dataTable;
            }
        }

        private static void ProcessRowGroup(DataTable dataTable, Parquet.ParquetRowGroupReader groupReader, List<Parquet.Data.DataField> fields, long skipRecords, long readRecords)
        {
            int rowBeginIndex = dataTable.Rows.Count;
            bool isFirstColumn = true;

            foreach (var field in fields)
            {
                int rowIndex = rowBeginIndex;

                int skippedRecords = 0;
                foreach (var value in groupReader.ReadColumn(field).Data)
                {
                    if (skipRecords > skippedRecords)
                    {
                        skippedRecords++;
                        continue;
                    }

                    if (rowIndex - rowBeginIndex >= readRecords)
                        break;

                    if (isFirstColumn)
                    {
                        var newRow = dataTable.NewRow();
                        dataTable.Rows.Add(newRow);
                    }

                    if (value == null)
                        dataTable.Rows[rowIndex][field.Name] = DBNull.Value;
                    else if (field.DataType == Parquet.Data.DataType.DateTimeOffset)
                        dataTable.Rows[rowIndex][field.Name] = ((DateTimeOffset)value).DateTime;
                    else
                        dataTable.Rows[rowIndex][field.Name] = value;

                    rowIndex++;
                }

                isFirstColumn = false;
            }
        }

        public static Type ParquetNetTypeToCSharpType(Parquet.Data.DataType type)
        {
            //TODO: Create static readonly Dictionary<Parquet.Data.DataType, Type>?
            Type columnType = null;
            switch (type)
            {
                case Parquet.Data.DataType.Boolean:
                    columnType = typeof(bool);
                    break;
                case Parquet.Data.DataType.Byte:
                    columnType = typeof(sbyte);
                    break;
                case Parquet.Data.DataType.ByteArray:
                    columnType = typeof(sbyte[]);
                    break;
                case Parquet.Data.DataType.DateTimeOffset:
                    //Let's treat DateTimeOffsets as DateTime
                    columnType = typeof(DateTime);
                    break;
                case Parquet.Data.DataType.Decimal:
                    columnType = typeof(decimal);
                    break;
                case Parquet.Data.DataType.Double:
                    columnType = typeof(double);
                    break;
                case Parquet.Data.DataType.Float:
                    columnType = typeof(float);
                    break;
                case Parquet.Data.DataType.Short:
                case Parquet.Data.DataType.Int16:
                case Parquet.Data.DataType.Int32:
                case Parquet.Data.DataType.UnsignedInt16:
                    columnType = typeof(int);
                    break;
                case Parquet.Data.DataType.Int64:
                    columnType = typeof(long);
                    break;
                case Parquet.Data.DataType.UnsignedByte:
                    columnType = typeof(byte);
                    break;
                case Parquet.Data.DataType.String:
                default:
                    columnType = typeof(string);
                    break;
            }

            return columnType;
        }

        public void Dispose()
        {
            foreach (var reader in this.readers)
            {
                try
                {
                    reader.Dispose();
                }
                catch (Exception) { }
            }
        }
    }
}
