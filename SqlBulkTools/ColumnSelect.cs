using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace SqlBulkTools
{
    public class ColumnSelect<T>
    {
        private readonly List<T> _list;
        private readonly string _tableName;
        private readonly string _schema;
        private readonly string _sourceAlias;
        private readonly string _targetAlias;
        private readonly int _sqlTimeout;
        private readonly int _bulkCopyTimeout;
        private readonly bool _bulkCopyEnableStreaming;
        private readonly int? _bulkCopyNotifyAfter;
        private readonly int? _bulkCopyBatchSize;
        private readonly SqlBulkTools _ext;
        private Dictionary<string, string> CustomColumnMappings { get; set; }
        private SqlBulkToolsHelpers _helper;
        private HashSet<string> _columns;
        

        public ColumnSelect(List<T> list, string tableName, HashSet<string> columns, string schema, string sourceAlias, string targetAlias, 
            int sqlTimeout, int bulkCopyTimeout, bool bulkCopyEnableStreaming, int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, 
            SqlBulkTools ext)
        {
            _helper = new SqlBulkToolsHelpers();
            _list = list;
            _columns = columns;
            _tableName = tableName;
            _schema = schema;
            _sourceAlias = sourceAlias;
            _targetAlias = targetAlias;
            _sqlTimeout = sqlTimeout;
            _bulkCopyTimeout = bulkCopyTimeout;
            _bulkCopyEnableStreaming = bulkCopyEnableStreaming;
            _bulkCopyNotifyAfter = bulkCopyNotifyAfter;
            _bulkCopyBatchSize = bulkCopyBatchSize;
            _ext = ext;
            CustomColumnMappings = new Dictionary<string, string>();
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
            _columns.Add(propertyName);

            return new ColumnSelect<T>(_list, _tableName, _columns, _schema, _sourceAlias, _targetAlias, _sqlTimeout, 
                _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter, _bulkCopyBatchSize, 
                _ext);
        }

        /// <summary>
        /// By default will attempt to match model property names to SQL column names. If your model does not match 
        /// SQL table column(s) then use this method to set up a custom mapping.  
        /// </summary>
        /// <param name="source">
        /// The object member that has a different name in SQL table. 
        /// </param>
        /// <param name="destination">
        /// The actual name of column as represented in SQL table. 
        /// </param>
        /// <returns></returns>
        public ColumnSelect<T> CustomColumnMapping(Expression<Func<T, object>> source, string destination)
        {
            var propertyName = _helper.GetPropertyName(source);
            CustomColumnMappings.Add(propertyName, destination);
            return this;
        }

        public BulkInsert<T> BulkInsert()
        {
            return new BulkInsert<T>(_list, _tableName, _schema, _columns, _sourceAlias, _targetAlias, 
                CustomColumnMappings, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter, 
                _bulkCopyBatchSize, _ext);
        }

        public BulkInsertOrUpdate<T> BulkInsertOrUpdate()
        {
            return new BulkInsertOrUpdate<T>(_list, _tableName, _schema, _columns, _sourceAlias, _targetAlias,
                CustomColumnMappings, _sqlTimeout, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter, 
                _bulkCopyBatchSize, _ext);
        }

        public BulkUpdate<T> BulkUpdate()
        {
            return new BulkUpdate<T>(_list, _tableName, _schema, _columns, _sourceAlias, _targetAlias, 
                CustomColumnMappings, _sqlTimeout, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter, 
                _bulkCopyBatchSize, _ext);
        }

        public BulkDelete<T> BulkDelete()
        {
            return new BulkDelete<T>(_list, _tableName, _schema, _columns, _sourceAlias, _targetAlias, CustomColumnMappings, 
                _sqlTimeout, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter, _bulkCopyBatchSize, _ext);
        } 

    }
}
