using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class BulkInsert<T> : ITransaction
    {
        private readonly IEnumerable<T> _list;
        private readonly string _tableName;
        private readonly string _schema;
        private readonly HashSet<string> _columns;
        private readonly List<string> _updateOnList;
        private readonly string _sourceAlias;
        private readonly string _targetAlias;
        private readonly BulkOperations _ext;
        private readonly BulkOperationsHelpers _helper;
        private readonly Dictionary<string, string> _customColumnMappings;
        private readonly int _bulkCopyTimeout;
        private readonly bool _bulkCopyEnableStreaming;
        private readonly int? _bulkCopyNotifyAfter;
        private readonly int? _bulkCopyBatchSize;
        private readonly HashSet<string> _disableIndexList;
        private readonly bool _disableAllIndexes;
        private readonly SqlBulkCopyOptions _sqlBulkCopyOptions;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        /// <param name="columns"></param>
        /// <param name="disableIndexList"></param>
        /// <param name="disableAllIndexes"></param>
        /// <param name="sourceAlias"></param>
        /// <param name="targetAlias"></param>
        /// <param name="customColumnMappings"></param>
        /// <param name="bulkCopyTimeout"></param>
        /// <param name="bulkCopyEnableStreaming"></param>
        /// <param name="bulkCopyNotifyAfter"></param>
        /// <param name="bulkCopyBatchSize"></param>
        /// <param name="sqlBulkCopyOptions"></param>
        /// <param name="ext"></param>
        public BulkInsert(IEnumerable<T> list, string tableName, string schema, HashSet<string> columns, HashSet<string> disableIndexList, bool disableAllIndexes, string sourceAlias,
            string targetAlias, Dictionary<string, string> customColumnMappings, int bulkCopyTimeout, bool bulkCopyEnableStreaming,
            int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, SqlBulkCopyOptions sqlBulkCopyOptions, BulkOperations ext)
        {
            _list = list;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _sourceAlias = sourceAlias;
            _targetAlias = targetAlias;
            _helper = new BulkOperationsHelpers();
            _updateOnList = new List<string>();
            _disableIndexList = disableIndexList;
            _disableAllIndexes = disableAllIndexes;
            _customColumnMappings = customColumnMappings;
            _bulkCopyTimeout = bulkCopyTimeout;
            _bulkCopyEnableStreaming = bulkCopyEnableStreaming;
            _bulkCopyNotifyAfter = bulkCopyNotifyAfter;
            _bulkCopyBatchSize = bulkCopyBatchSize;
            _ext = ext;
            _sqlBulkCopyOptions = sqlBulkCopyOptions;
            _ext.SetBulkExt(this);
        }



        void ITransaction.CommitTransaction(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            if (!_list.Any())
            {
                return;
            }

            if (_disableAllIndexes && (_disableIndexList != null && _disableIndexList.Any()))
            {
                throw new InvalidOperationException("Invalid setup. If \'TmpDisableAllNonClusteredIndexes\' is invoked, you can not use the \'AddTmpDisableNonClusteredIndex\' method.");
            }

            DataTable dt = _helper.ToDataTable(_list, _columns, _customColumnMappings);

            // Must be after ToDataTable is called. 
            _helper.DoColumnMappings(_customColumnMappings, _columns, _updateOnList);

            using (SqlConnection conn = _helper.GetSqlConnection(connectionName, credentials, connection))
            {

                conn.Open();

                using (SqlTransaction transaction = conn.BeginTransaction())
                {
                    //Bulk insert into temp table
                    using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn, _sqlBulkCopyOptions, transaction))
                    {
                        try
                        {
                            bulkcopy.DestinationTableName = _helper.GetFullQualifyingTableName(conn.Database, _schema, _tableName);
                            _helper.MapColumns(bulkcopy, _columns, _customColumnMappings);

                            _helper.SetSqlBulkCopySettings(bulkcopy, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
                                _bulkCopyNotifyAfter, _bulkCopyTimeout);

                            SqlCommand command = conn.CreateCommand();

                            command.Connection = conn;
                            command.Transaction = transaction;

                            if (_disableAllIndexes || (_disableIndexList != null && _disableIndexList.Any()))
                            {
                                command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Disable, _tableName, _disableIndexList, _disableAllIndexes);
                                command.ExecuteNonQuery();
                            }

                            bulkcopy.WriteToServer(dt);

                            if (_disableAllIndexes || (_disableIndexList != null && _disableIndexList.Any()))
                            {
                                command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Rebuild, _tableName, _disableIndexList, _disableAllIndexes);
                                command.ExecuteNonQuery();
                            }

                            transaction.Commit();

                            bulkcopy.Close();
                        }

                        catch (Exception)
                        {
                            transaction.Rollback();
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionName"></param>
        /// <param name="credentials"></param>
        /// <param name="connection"></param>
        /// <returns></returns>
        async Task ITransaction.CommitTransactionAsync(string connectionName, SqlCredential credentials, SqlConnection connection)
        {
            if (!_list.Any())
            {
                return;
            }

            if (_disableAllIndexes && (_disableIndexList != null && _disableIndexList.Any()))
            {
                throw new InvalidOperationException("Invalid setup. If \'TmpDisableAllNonClusteredIndexes\' is invoked, you can not use the \'AddTmpDisableNonClusteredIndex\' method.");
            }

            DataTable dt = _helper.ToDataTable(_list, _columns, _customColumnMappings);

            // Must be after ToDataTable is called. 
            _helper.DoColumnMappings(_customColumnMappings, _columns, _updateOnList);

            using (SqlConnection conn = _helper.GetSqlConnection(connectionName, credentials, connection))
            {

                conn.Open();

                using (SqlTransaction transaction = conn.BeginTransaction())
                {
                    //Bulk insert into temp table
                    using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn, _sqlBulkCopyOptions, transaction))
                    {
                        try
                        {
                            bulkcopy.DestinationTableName = _helper.GetFullQualifyingTableName(conn.Database, _schema, _tableName);
                            _helper.MapColumns(bulkcopy, _columns, _customColumnMappings);

                            _helper.SetSqlBulkCopySettings(bulkcopy, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
                                _bulkCopyNotifyAfter, _bulkCopyTimeout);

                            SqlCommand command = conn.CreateCommand();

                            command.Connection = conn;
                            command.Transaction = transaction;

                            if (_disableAllIndexes || (_disableIndexList != null && _disableIndexList.Any()))
                            {
                                command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Disable, _tableName, _disableIndexList, _disableAllIndexes);
                                await command.ExecuteNonQueryAsync();
                            }

                            await bulkcopy.WriteToServerAsync(dt);

                            if (_disableAllIndexes || (_disableIndexList != null && _disableIndexList.Any()))
                            {
                                command.CommandText = _helper.GetIndexManagementCmd(IndexOperation.Rebuild, _tableName, _disableIndexList, _disableAllIndexes);
                                await command.ExecuteNonQueryAsync();
                            }
                            transaction.Commit();

                            bulkcopy.Close();
                        }

                        catch (Exception)
                        {
                            transaction.Rollback();
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
}
