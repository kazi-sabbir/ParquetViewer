using System;
using System.Collections.Generic;
using System.Data;

namespace ParquetFileViewer
{
    public static class ExtensionMethods
    {
        public static DataTable SetColumnsOrder(this DataTable table, IEnumerable<string> columnNames)
        {
            int columnIndex = 0;
            foreach (var columnName in columnNames)
            {
                table.Columns[columnName].SetOrdinal(columnIndex);
                columnIndex++;
            }

            return table;
        }
    }
}
