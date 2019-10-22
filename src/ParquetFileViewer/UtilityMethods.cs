using Parquet;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace ParquetFileViewer
{
    public static class UtilityMethods
    {
        public static List<string> GetDataTableColumns(DataTable datatable)
        {
            List<string> columns = new List<string>(datatable != null ? datatable.Columns.Count : 0);
            if (datatable != null)
            {
                foreach (DataColumn column in datatable.Columns)
                {
                    columns.Add(column.ColumnName);
                }
            }
            return columns;
        }

        public static string CleanCSVValue(string value, bool alwaysEncloseInQuotes = false)
        {
            if (!string.IsNullOrWhiteSpace(value))
            {
                //In RFC 4180 we escape quotes with double quotes
                string formattedValue = value.Replace("\"", "\"\"");

                //Enclose value with quotes if it contains commas,line feeds or other quotes
                if (formattedValue.Contains(",") || formattedValue.Contains("\r") || formattedValue.Contains("\n") || formattedValue.Contains("\"\"") || alwaysEncloseInQuotes)
                    formattedValue = string.Concat("\"", formattedValue, "\"");

                return formattedValue;
            }
            else
                return string.Empty;
        }
    }
}
