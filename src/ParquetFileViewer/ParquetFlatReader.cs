using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ParquetFileViewer
{
    public class ParquetFlatReader : IDisposable
    {
        List<Parquet.ParquetReader> readers = new List<Parquet.ParquetReader>();

        private Parquet.ParquetReader defaultReader { get { return this.readers[0]; } }

        public ParquetFlatReader(string filePath, int degreeOfParallelism = 1, Parquet.ParquetOptions parquetOptions = null)
        {
            if (!File.Exists(filePath))
                throw new FileNotFoundException(filePath);

            if (degreeOfParallelism <= 0)
                throw new ArgumentException("degreeOfParallelism must be a positive integer");

            for (int i = 0; i < degreeOfParallelism; i++)
            {
                readers.Add(
                    new Parquet.ParquetReader(new FileStream(filePath, FileMode.Open, FileAccess.Read),
                        parquetOptions,
                        false)
                    );
            }
        }



        public long GetRowCount()
        {
            long count = 0;
            for (int i = 0; i < this.defaultReader.RowGroupCount; i++)
            {
                using (var groupReader = this.defaultReader.OpenRowGroupReader(i))
                {
                    count += groupReader.RowCount;
                }
            }

            return count;
        }

        public DataTable ToDataTable(List<string> selectedFields, long offset, long recordCount)
        {
            //Get list of data fields and construct the DataTable
            DataTable dataTable = new DataTable();
            List<Parquet.Data.DataField> fields = new List<Parquet.Data.DataField>();
            var dataFields = this.defaultReader.Schema.GetDataFields();
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

            //Read column by column to generate each row in the datatable
            for (int i = 0; i < this.defaultReader.RowGroupCount; i++)
            {
                bool readGroup = false;
                long rowsLeftToRead = recordCount;
                long rowsPassedUntilThisRowGroup = -1;
                long groupRowCount = -1;
                using (var groupReader = this.defaultReader.OpenRowGroupReader(i))
                {
                    rowsPassedUntilThisRowGroup = totalRecordCount;
                    totalRecordCount += (int)groupReader.RowCount;

                    if (offset >= totalRecordCount)
                        continue;

                    if (rowsLeftToRead > 0)
                        readGroup = true;

                    groupRowCount = groupReader.RowCount;
                }

                if (readGroup)
                {
                    //TODO:Split fields by degree of parallelism

                    long numberOfRecordsToReadFromThisRowGroup = Math.Min(Math.Min(totalRecordCount - offset, recordCount), groupRowCount);
                    rowsLeftToRead -= numberOfRecordsToReadFromThisRowGroup;

                    long recordsToSkipInThisRowGroup = Math.Max(offset - rowsPassedUntilThisRowGroup, 0);

                    List<Task> readOperations = new List<Task>();
                    foreach (var reader in this.readers)
                    {
                        readOperations.Add(Task.Run(() =>
                        {
                            using (var groupReader = reader.OpenRowGroupReader(i))
                            {
                                ProcessRowGroup(dataTable, groupReader, fields, recordsToSkipInThisRowGroup, numberOfRecordsToReadFromThisRowGroup);
                            }
                        }));
                    }
                }
            }

            return dataTable;
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

                    if (rowIndex >= readRecords)
                        break;

                    if (isFirstColumn)
                    {
                        var newRow = dataTable.NewRow();
                        dataTable.Rows.Add(newRow);
                    }

                    if (value == null)
                        dataTable.Rows[rowIndex][field.Name] = DBNull.Value;
                    else if (field.DataType == Parquet.Data.DataType.DateTimeOffset)
                        dataTable.Rows[rowIndex][field.Name] = ((DateTimeOffset)value).DateTime; //converts to local time!
                    else
                        dataTable.Rows[rowIndex][field.Name] = value;

                    rowIndex++;
                }

                isFirstColumn = false;
            }
        }

        public static Type ParquetNetTypeToCSharpType(Parquet.Data.DataType type)
        {
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
