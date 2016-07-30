using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq.Expressions;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ColumnSelect<T>
    {
        private readonly IEnumerable<T> _list;
        private readonly string _tableName;
        private readonly string _schema;
        private readonly string _sourceAlias;
        private readonly string _targetAlias;
        private readonly int _sqlTimeout;
        private readonly int _bulkCopyTimeout;
        private readonly bool _bulkCopyEnableStreaming;
        private readonly int? _bulkCopyNotifyAfter;
        private readonly int? _bulkCopyBatchSize;
        private readonly BulkOperations _ext;
        private Dictionary<string, string> CustomColumnMappings { get; set; }
        private readonly BulkOperationsHelpers _helper;
        private readonly HashSet<string> _columns;
        private bool _disableAllIndexes;
        private readonly HashSet<string> _disableIndexList;
        private readonly SqlBulkCopyOptions _sqlBulkCopyOptions;


        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="columns"></param>
        /// <param name="schema"></param>
        /// <param name="sourceAlias"></param>
        /// <param name="targetAlias"></param>
        /// <param name="sqlTimeout"></param>
        /// <param name="bulkCopyTimeout"></param>
        /// <param name="bulkCopyEnableStreaming"></param>
        /// <param name="bulkCopyNotifyAfter"></param>
        /// <param name="bulkCopyBatchSize"></param>
        /// <param name="sqlBulkCopyOptions"></param>
        /// <param name="ext"></param>
        public ColumnSelect(IEnumerable<T> list, string tableName, HashSet<string> columns, string schema, string sourceAlias, string targetAlias, 
            int sqlTimeout, int bulkCopyTimeout, bool bulkCopyEnableStreaming, int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, SqlBulkCopyOptions sqlBulkCopyOptions, 
            BulkOperations ext)
        {
            _helper = new BulkOperationsHelpers();
            _list = list;
            _columns = columns;
            _disableAllIndexes = false;
            _disableIndexList = new HashSet<string>();
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
            _sqlBulkCopyOptions = sqlBulkCopyOptions;
            CustomColumnMappings = new Dictionary<string, string>();
        }


        /// <summary>
        /// Add each column that you want to include in the query. Only include the columns that are relevant to the 
        /// procedure for best performance. 
        /// </summary>
        /// <param name="columnName">Column name as represented in database</param>
        /// <returns></returns>
        public ColumnSelect<T> AddColumn(Expression<Func<T, object>> columnName)
        {
            var propertyName = _helper.GetPropertyName(columnName);
            _columns.Add(propertyName);
            return this;
        }

        /// <summary>
        /// By default SqlBulkTools will attempt to match the model property names to SQL column names (case insensitive). 
        /// If any of your model property names do not match 
        /// the SQL table column(s) as defined in given table, then use this method to set up a custom mapping.  
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

        /// <summary>
        /// Disables non-clustered index. You can select One to Many non-clustered indexes. This option should be considered on 
        /// a case-by-case basis. Understand the consequences before using this option.  
        /// </summary>
        /// <param name="indexName"></param>
        /// <returns></returns>
        public ColumnSelect<T> AddTmpDisableNonClusteredIndex(string indexName)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _disableIndexList.Add(indexName);

            return this;
        }

        /// <summary>
        /// Disables all Non-Clustered indexes on the table before the transaction and rebuilds after the 
        /// transaction. This option should be considered on a case-by-case basis. Understand the 
        /// consequences before using this option.  
        /// </summary>
        /// <returns></returns>
        public ColumnSelect<T> TmpDisableAllNonClusteredIndexes()
        {
            _disableAllIndexes = true;
            return this;
        }

        /// <summary>
        /// A bulk insert will attempt to insert all records. If you have any unique constraints on columns, these must be respected. 
        /// Notes: (1) Only the columns configured (via AddColumn) will be evaluated. (3) Use AddAllColumns to add all columns in table. 
        /// </summary>
        /// <returns></returns>
        public BulkInsert<T> BulkInsert()
        {
            return new BulkInsert<T>(_list, _tableName, _schema, _columns, _disableIndexList, _disableAllIndexes, _sourceAlias, _targetAlias, 
                CustomColumnMappings, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter, 
                _bulkCopyBatchSize, _sqlBulkCopyOptions, _ext);
        }

        /// <summary>
        /// A bulk insert or update is also known as bulk upsert or merge. All matching rows from the source will be updated.
        /// Any unique rows not found in target but exist in source will be added. Notes: (1) BulkInsertOrUpdate requires at least 
        /// one MatchTargetOn property to be configured. (2) Only the columns configured (via AddColumn) 
        /// will be evaluated. (3) Use AddAllColumns to add all columns in table.
        /// </summary>
        /// <returns></returns>
        public BulkInsertOrUpdate<T> BulkInsertOrUpdate()
        {
            return new BulkInsertOrUpdate<T>(_list, _tableName, _schema, _columns, _disableIndexList, _disableAllIndexes, _sourceAlias, _targetAlias,
                CustomColumnMappings, _sqlTimeout, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter, 
                _bulkCopyBatchSize, _sqlBulkCopyOptions, _ext);
        }

        /// <summary>
        /// A bulk update will attempt to update any matching records. Notes: (1) BulkUpdate requires at least one MatchTargetOn 
        /// property to be configured. (2) Only the columns configured (via AddColumn) will be evaluated. (3) Use AddAllColumns to add all columns in table.
        /// </summary>
        /// <returns></returns>
        public BulkUpdate<T> BulkUpdate()
        {
            return new BulkUpdate<T>(_list, _tableName, _schema, _columns, _disableIndexList, _disableAllIndexes, _sourceAlias, _targetAlias, 
                CustomColumnMappings, _sqlTimeout, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter, 
                _bulkCopyBatchSize, _sqlBulkCopyOptions, _ext);
        }

        /// <summary>
        /// A bulk delete will delete records when matched. Consider using a DTO with only the needed information (e.g. PK) Notes: 
        /// (1) BulkUpdate requires at least one MatchTargetOn property to be configured. 
        /// </summary>
        /// <returns></returns>
        public BulkDelete<T> BulkDelete()
        {
            return new BulkDelete<T>(_list, _tableName, _schema, _columns, _disableIndexList, _disableAllIndexes, _sourceAlias, _targetAlias, CustomColumnMappings, 
                _sqlTimeout, _bulkCopyTimeout, _bulkCopyEnableStreaming, _bulkCopyNotifyAfter, _bulkCopyBatchSize, _sqlBulkCopyOptions, _ext);
        } 

    }
}
