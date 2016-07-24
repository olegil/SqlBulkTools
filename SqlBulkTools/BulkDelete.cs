using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace SqlBulkTools
{
    public class BulkDelete<T> : ITransaction
    {
        private readonly ICollection<T> _list; 
        private readonly string _tableName;
        private readonly string _schema;
        private readonly HashSet<string> _columns;
        private readonly List<string> _matchTargetOn;
        private readonly string _sourceAlias;
        private readonly string _targetAlias;
        private readonly BulkOperationsHelpers _helper;
        private readonly Dictionary<string, string> _customColumnMappings;
        private readonly int _sqlTimeout;
        private readonly int _bulkCopyTimeout;
        private readonly bool _bulkCopyEnableStreaming;
        private readonly int? _bulkCopyNotifyAfter;
        private readonly int? _bulkCopyBatchSize;
        private readonly BulkOperations _ext;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="columns"></param>
        /// <param name="sourceAlias"></param>
        /// <param name="targetAlias"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="sqlTimeout"></param>
        /// <param name="bulkCopyTimeout"></param>
        /// <param name="bulkCopyEnableStreaming"></param>
        /// <param name="bulkCopyNotifyAfter"></param>
        /// <param name="bulkCopyBatchSize"></param>
        /// <param name="ext"></param>
        public BulkDelete(ICollection<T> list, string tableName, string schema, HashSet<string> columns, string sourceAlias, 
            string targetAlias, Dictionary<string, string> customColumnMappings, int sqlTimeout, int bulkCopyTimeout, 
            bool bulkCopyEnableStreaming, int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, BulkOperations ext)
        {
            _list = list;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _matchTargetOn = new List<string>();
            _sourceAlias = sourceAlias;
            _targetAlias = targetAlias;
            _customColumnMappings = customColumnMappings;
            _sqlTimeout = sqlTimeout;
            _bulkCopyTimeout = bulkCopyTimeout;
            _bulkCopyEnableStreaming = bulkCopyEnableStreaming;
            _bulkCopyNotifyAfter = bulkCopyNotifyAfter;
            _bulkCopyBatchSize = bulkCopyBatchSize;
            _helper = new BulkOperationsHelpers();
            _ext = ext;           
            _ext.SetBulkExt(this);
        }

        /// <summary>
        /// At least one MatchTargetOn is required for correct configuration. MatchTargetOn is the matching clause for evaluating 
        /// each row in table. This is usally set to the unique identifier in the table (e.g. Id). Multiple MatchTargetOn members are allowed 
        /// for matching composite relationships. 
        /// </summary>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public BulkDelete<T> MatchTargetOn(Expression<Func<T, object>> columnName)
        {
            var propertyName = _helper.GetPropertyName(columnName);
            _matchTargetOn.Add(propertyName);
            return this;
        }

        void ITransaction.CommitTransaction(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            if (_list.Count == 0)
            {
                return;
            }

            if (_matchTargetOn.Count == 0)
            {
                throw new InvalidOperationException("MatchTargetOn list is empty when it's required for this operation. This is usually the primary key of your table but can also be more than one column depending on your business rules.");
            }

            DataTable dt = _helper.ToDataTable(_list, _columns, _customColumnMappings, _matchTargetOn);

            // Must be after ToDataTable is called. 
            _helper.DoColumnMappings(_customColumnMappings, _columns);
            ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.PerUserRoamingAndLocal);

            using (SqlConnection conn = _helper.GetSqlConnection(connectionName, credentials, connection))
            {
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    try
                    {
                        conn.Open();
                        var dtCols = _helper.GetSchema(conn, _schema, _tableName);

                        //Creating temp table on database
                        command.CommandText = _helper.BuildCreateTempTable(_columns, dtCols);
                        command.ExecuteNonQuery();

                        _helper.InsertToTmpTable(conn, dt, _bulkCopyEnableStreaming, _bulkCopyBatchSize, _bulkCopyNotifyAfter, _bulkCopyTimeout);

                        // Updating destination table, and dropping temp table
                        command.CommandTimeout = _sqlTimeout;
                        string comm = "BEGIN TRAN; " + 
                                      "MERGE INTO " + _tableName + " AS Target " +
                                      "USING #TmpTable AS Source " +
                                      _helper.BuildJoinConditionsForUpdateOrInsert(_matchTargetOn.ToArray(), 
                                      _sourceAlias, _targetAlias) +
                                      "WHEN MATCHED THEN DELETE; " +
                                      "DROP TABLE #TmpTable; COMMIT TRAN;";
                        command.CommandText = comm;
                        command.ExecuteNonQuery();
                    
                    }
                    catch (Exception e)
                    {
                        command.CommandText = "IF @@TRANCOUNT > 0 ROLLBACK;";
                        command.ExecuteNonQuery();                        
                        throw;
                    }
                    finally
                    {
                        conn.Close();
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="credentials"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        async Task ITransaction.CommitTransactionAsync(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            if (_list.Count == 0)
            {
                return;
            }

            if (_matchTargetOn.Count == 0)
            {
                throw new InvalidOperationException("MatchTargetOn list is empty when it's required for this operation. This is usually the primary key of your table but can also be more than one column depending on your business rules.");
            }

            DataTable dt = _helper.ToDataTable(_list, _columns, _customColumnMappings, _matchTargetOn);

            // Must be after ToDataTable is called. 
            _helper.DoColumnMappings(_customColumnMappings, _columns);

            ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.PerUserRoamingAndLocal);

            using (SqlConnection conn = _helper.GetSqlConnection(connectionName, credentials, connection))
            {
                using (SqlCommand command = new SqlCommand("", conn))
                {
                    try
                    {
                        conn.Open();
                        var dtCols = _helper.GetSchema(conn, _schema, _tableName);

                        //Creating temp table on database
                        command.CommandText = _helper.BuildCreateTempTable(_columns, dtCols);
                        await command.ExecuteNonQueryAsync();

                        await _helper.InsertToTmpTableAsync(conn, dt, _bulkCopyEnableStreaming, _bulkCopyBatchSize, _bulkCopyNotifyAfter, _bulkCopyTimeout);

                        // Updating destination table, and dropping temp table
                        command.CommandTimeout = _sqlTimeout;
                        string comm = "BEGIN TRAN; " +
                                      "MERGE INTO " + _tableName + " AS Target " +
                                      "USING #TmpTable AS Source " +
                                      _helper.BuildJoinConditionsForUpdateOrInsert(_matchTargetOn.ToArray(),
                                      _sourceAlias, _targetAlias) +
                                      "WHEN MATCHED THEN DELETE; " +
                                      "DROP TABLE #TmpTable; COMMIT TRAN;";
                        command.CommandText = comm;
                        await command.ExecuteNonQueryAsync();

                    }
                    catch (Exception e)
                    {
                        command.CommandText = "IF @@TRANCOUNT > 0 ROLLBACK;";
                        await command.ExecuteNonQueryAsync();
                        throw;
                    }
                    finally
                    {
                        conn.Close();
                    }
                }
            }
        }
    }
}
