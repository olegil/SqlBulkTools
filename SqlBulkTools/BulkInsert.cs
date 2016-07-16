using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace SqlBulkTools
{
    public class BulkInsert<T> : ITransaction
    {
        private readonly List<T> _list;
        private readonly string _tableName;
        private readonly string _schema;
        private readonly HashSet<string> _columns;
        private readonly List<string> _updateOnList;
        private readonly string _sourceAlias;
        private readonly string _targetAlias;
        private readonly SqlBulkTools _ext;
        private readonly SqlBulkToolsHelpers _helper;
        private readonly Dictionary<string, string> _customColumnMappings;
        private readonly int _bulkCopyTimeout;
        private readonly bool _bulkCopyEnableStreaming;
        private readonly int? _bulkCopyNotifyAfter;
        private readonly int? _bulkCopyBatchSize;

        public BulkInsert(List<T> list, string tableName, string schema, HashSet<string> columns, string sourceAlias, 
            string targetAlias, Dictionary<string, string> customColumnMappings, int bulkCopyTimeout, bool bulkCopyEnableStreaming, 
            int? bulkCopyNotifyAfter, int? bulkCopyBatchSize, SqlBulkTools ext)
        {
            _list = list;
            _tableName = tableName;
            _schema = schema;
            _columns = columns;
            _sourceAlias = sourceAlias;
            _targetAlias = targetAlias;            
            _helper = new SqlBulkToolsHelpers();
            _updateOnList = new List<string>();
            _customColumnMappings = customColumnMappings;
            _bulkCopyTimeout = bulkCopyTimeout;
            _bulkCopyEnableStreaming = bulkCopyEnableStreaming;
            _bulkCopyNotifyAfter = bulkCopyNotifyAfter;
            _bulkCopyBatchSize = bulkCopyBatchSize;
            _ext = ext;
            _ext.SetBulkExt(this);
        }

        public void CommitTransaction(string connectionString, SqlCredential credentials)
        {
            if (_list.Count == 0)
            {
                throw new ArgumentException("The collection provided does not contain any objects.");
            }

            DataTable dt = _helper.ToDataTable(_list, _columns, _customColumnMappings);

            // Must be after ToDataTable is called. 
            _helper.DoColumnMappings(_customColumnMappings, _columns, _updateOnList);

            ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.PerUserRoamingAndLocal);
            using (SqlConnection conn = new SqlConnection(ConfigurationManager
                .ConnectionStrings[connectionString].ConnectionString, credentials))
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

                        //Bulk insert into temp table
                        using (SqlBulkCopy bulkcopy = new SqlBulkCopy(conn))
                        {
                            
                            bulkcopy.DestinationTableName = _tableName;
                            _helper.MapColumns(bulkcopy, _columns, _customColumnMappings);

                            _helper.SetSqlBulkCopySettings(bulkcopy, _bulkCopyEnableStreaming, _bulkCopyBatchSize,
                                _bulkCopyNotifyAfter, _bulkCopyTimeout);

                            bulkcopy.WriteToServer(dt);
                            bulkcopy.Close();
                        }
                    }
                    catch (Exception)
                    {
                        command.CommandText = "ROLLBACK Transaction;";
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
    }
}
