using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class DataTable<T>
    {
        private readonly BulkOperationsHelpers _helper;
        private HashSet<string> Columns { get; set; }
        private readonly IEnumerable<T> _list;

        /// <summary>
        /// 
        /// </summary>
        public DataTable(IEnumerable<T> list)
        {
            _helper = new BulkOperationsHelpers();
            _list = list;            
            Columns = new HashSet<string>();
        }

        /// <summary>
        /// Add each column that you want to include in the DataTable.
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public DataTableColumnSelect<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = _helper.GetPropertyName(columnName);
            Columns.Add(propertyName);
            return new DataTableColumnSelect<T>(_list, Columns);
        }

        /// <summary>
        /// Adds all properties in model that are either value or string type into the DataTable. 
        /// </summary>
        /// <returns></returns>
        public DataTableAllColumnSelect<T> AddAllColumns()
        {
            Columns = _helper.GetAllValueTypeAndStringColumns(typeof(T));
            return new DataTableAllColumnSelect<T>(_list, Columns);
        }

    }
}
