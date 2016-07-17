using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace SqlBulkTools
{
    public class Table<T>
    {
        private HashSet<string> Columns { get; set; }
        private List<string> UpdateOnList { get; set; }
        private List<string> DeleteOnList { get; set; }
        private readonly BulkOperationsHelpers _helper;
        private string _schema;
        private readonly string _tableName;
        private readonly string _sourceAlias;
        private readonly string _targetAlias;
        private readonly BulkOperations _ext;
        private readonly ICollection<T> _list;
        private Dictionary<string, string> CustomColumnMappings { get; set; }
        private int _sqlTimeout;
        private int _bulkCopyTimeout;
        private bool _bulkCopyEnableStreaming;
        private int? _bulkCopyNotifyAfter;
        private int? _bulkCopyBatchSize;

        public Table(ICollection<T> list, string tableName, string sourceAlias, string targetAlias, BulkOperations ext)
        {
            _bulkCopyBatchSize = null;
            _bulkCopyNotifyAfter = null;
            _bulkCopyEnableStreaming = false;
            _helper = new BulkOperationsHelpers();
            _tableName = tableName;
            _sourceAlias = sourceAlias;
            _targetAlias = targetAlias;
            _sqlTimeout = 600;
            _bulkCopyTimeout = 600;
            _ext = ext;
            _list = list;
            _schema = null;
            Columns = new HashSet<string>();
            UpdateOnList = new List<string>();
            DeleteOnList = new List<string>();
            CustomColumnMappings = new Dictionary<string, string>();
        }

        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <param name="isIdentity"></param>
        /// <returns></returns>
        public ColumnSelect<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = _helper.GetPropertyName(columnName);
            Columns.Add(propertyName);
            return new ColumnSelect<T>(_list, _tableName, Columns, _schema, _sourceAlias, _targetAlias, 
                _sqlTimeout, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter, _bulkCopyBatchSize, _ext);
        }

        /// <summary>
        /// Explicitley set a schema if your table may have a naming conflict within your database. Adding a schema is optional. 
        /// </summary>
        /// <param name="schema"></param>
        /// <returns></returns>
        public Table<T> WithSchema(string schema)
        {
            _schema = schema;
            return this;
        }

        /// <summary>
        /// Default is 600 seconds. See docs for more info. 
        /// </summary>
        /// <param name="seconds"></param>
        /// <returns></returns>
        public Table<T> WithSqlCommandTimeout(int seconds)
        {
            _sqlTimeout = seconds;
            return this;
        }

        /// <summary>
        /// Default is 600 seconds. See docs for more info. 
        /// </summary>
        /// <param name="seconds"></param>
        /// <returns></returns>
        public Table<T> WithBulkCopyCommandTimeout(int seconds)
        {
            _bulkCopyTimeout = seconds;
            return this;
        }

        /// <summary>
        /// Default is false. See docs for more info.
        /// </summary>
        /// <param name="status"></param>
        /// <returns></returns>
        public Table<T> WithBulkCopyEnableStreaming(bool status)
        {
            _bulkCopyEnableStreaming = status;
            return this;
        }

        /// <summary>
        /// Triggers an event after x rows inserted. See docs for more info. 
        /// </summary>
        /// <param name="rows"></param>
        /// <returns></returns>
        public Table<T> WithBulkCopyNotifyAfter(int rows)
        {
            _bulkCopyNotifyAfter = rows;
            return this;
        }

        /// <summary>
        /// Default is 0. See docs for more info. 
        /// </summary>
        /// <param name="rows"></param>
        /// <returns></returns>
        public Table<T> WithBulkCopyBatchSize(int rows)
        {
            _bulkCopyBatchSize = rows;
            return this;
        }   

    }
}
