using System.Collections.Generic;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class CollectionSelect<T>
    {
        private readonly IEnumerable<T> _list;
        private readonly string _sourceAlias;
        private readonly string _targetAlias;
        private readonly BulkOperations _ext;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="sourceAlias"></param>
        /// <param name="targetAlias"></param>
        /// <param name="ext"></param>
        public CollectionSelect(IEnumerable<T> list, string sourceAlias, string targetAlias, BulkOperations ext)
        {
            _list = list;
            _sourceAlias = sourceAlias;
            _targetAlias = targetAlias;
            _ext = ext;
        }

        /// <summary>
        /// Set the name of table for operation to take place. Registering a table is Required.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public Table<T> WithTable(string tableName)
        {
            return new Table<T>(_list, tableName, _sourceAlias, _targetAlias, _ext);
        }
    }
}
