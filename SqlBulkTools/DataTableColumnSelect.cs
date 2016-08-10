using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq.Expressions;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DataTableColumnSelect<T>
    {
        private readonly IEnumerable<T> _list;
        private Dictionary<string, string> CustomColumnMappings { get; set; }
        private readonly BulkOperationsHelpers _helper;
        private readonly HashSet<string> _columns;


        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="columns"></param>
        public DataTableColumnSelect(IEnumerable<T> list, HashSet<string> columns)
        {
            _helper = new BulkOperationsHelpers();
            _list = list;
            _columns = columns;
            CustomColumnMappings = new Dictionary<string, string>();
        }


        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the 
        /// procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public DataTableColumnSelect<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = _helper.GetPropertyName(columnName);
            _columns.Add(propertyName);
            return this;
        }

        /// <summary>
        /// If a column name in your model does not match the designated column name in the actual SQL table, 
        /// you can add a custom column mapping. 
        /// </summary>
        /// <returns></returns>
        public DataTableColumnSelect<T> CustomColumnMapping(Expression<Func<T, object>> source, string destination)
        {
            var propertyName = _helper.GetPropertyName(source);
            CustomColumnMappings.Add(propertyName, destination);
            return this;
        }

        /// <summary>
        /// Returns a data table to be used in a stored procedure. 
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public DataTable ToDataTable()
        {
            return _helper.ToDataTable(_list, _columns, CustomColumnMappings);
        }

    }
}
