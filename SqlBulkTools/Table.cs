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
        private SqlBulkToolsHelpers _helper;
        private string _schema;
        private string _tableName;
        private readonly string _sourceAlias;
        private readonly string _targetAlias;
        private readonly global::SqlBulkTools.SqlBulkTools _ext;
        private readonly List<T> _list;
        private Dictionary<string, string> CustomColumnMappings { get; set; }
        private int _sqlTimeout;
        private int _bulkCopyTimeout;
        private bool _bulkCopyEnableStreaming;
        private int? _bulkCopyNotifyAfter;
        private int? _bulkCopyBatchSize;

        public Table(List<T> list, string tableName, string sourceAlias, string targetAlias, global::SqlBulkTools.SqlBulkTools ext)
        {
            _bulkCopyBatchSize = null;
            _bulkCopyNotifyAfter = null;
            _bulkCopyEnableStreaming = false;
            _helper = new SqlBulkToolsHelpers();
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

        public Table<T> WithBulkCopyEnableStreaming(bool status)
        {
            _bulkCopyEnableStreaming = status;
            return this;
        }

        public Table<T> WithSqlCommandTimeout(int seconds)
        {
            _sqlTimeout = seconds;
            return this;
        }

        public Table<T> WithBulkCopyCommandTimeout(int seconds)
        {
            _bulkCopyTimeout = seconds;
            return this;
        }   
        /// <summary>
        /// Triggers an event after x rows inserted
        /// </summary>
        /// <param name="rows"></param>
        /// <returns></returns>
        public Table<T> WithBulkCopyNotifyAfter(int rows)
        {
            _bulkCopyNotifyAfter = rows;
            return this;
        }

        /// <summary>
        /// Default is 0
        /// </summary>
        /// <param name="rows"></param>
        /// <returns></returns>
        public Table<T> WithBulkCopyBatchSize(int rows)
        {
            _bulkCopyBatchSize = rows;
            return this;
        }   

        /// <summary>
        /// Explicitley set a schema if your table may have a naming conflict. 
        /// </summary>
        /// <param name="schema">Adding a schema is Optional</param>
        /// <returns></returns>
        public Table<T> WithSchema(string schema)
        {
            _schema = schema;
            return this;
        }

        /// <summary>
        /// Add a column to be inserted/updated. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <param name="columnType">Column type must match represented type in SQL table. For example: nvarchar(max), nvarchar(256), int, bit, etc</param>
        /// <returns></returns>
        public ColumnSelect<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = _helper.GetPropertyName(columnName);
            Columns.Add(propertyName);
            return new ColumnSelect<T>(_list, _tableName, Columns, _schema, _sourceAlias, _targetAlias, _sqlTimeout, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter, _bulkCopyBatchSize, _ext);
        }


    }
}
