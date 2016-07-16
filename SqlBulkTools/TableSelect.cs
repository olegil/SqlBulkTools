using System.Collections.Generic;

namespace SqlBulkTools
{
    public class TableSelect<T>
    {
        private readonly List<T> _list;
        private readonly string _sourceAlias;
        private readonly string _targetAlias;
        private readonly global::SqlBulkTools.SqlBulkTools _ext;

        internal TableSelect(List<T> list, string sourceAlias, string targetAlias, global::SqlBulkTools.SqlBulkTools ext)
        {
            _list = list;
            _sourceAlias = sourceAlias;
            _targetAlias = targetAlias;
            _ext = ext;
        }

        /// <summary>
        /// Set the name of table for operation to take place. 
        /// </summary>
        /// <param name="table">Adding a table is Required</param>
        /// <returns></returns>
        public Table<T> WithTable(string table)
        {
            return new Table<T>(_list, table, _sourceAlias, _targetAlias, _ext);
        }
    }
}
