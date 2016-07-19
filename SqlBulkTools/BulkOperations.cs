using System;
using System.Configuration;
using System.Data.SqlClient;

namespace SqlBulkTools
{
    public class BulkOperations : ITransaction, IBulkOperations
    {
        private ITransaction _transaction;
        private const string SourceAlias = "Source";
        private const string TargetAlias = "Target";  

        internal void SetBulkExt(ITransaction transaction)
        {
            _transaction = transaction;
        }

        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for operation to be 
        /// successful. Notes(1): The connectionName parameter is a name that you provide to 
        /// uniquely identify a connection string so that it can be retrieved at run time
        /// </summary>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="InvalidOperationException"></exception>
        /// <param name="connectionName"></param>
        /// <param name="credentials"></param>
        public void CommitTransaction(string connectionName, SqlCredential credentials = null)
        {
            if (connectionName == null)
                throw new ArgumentNullException(nameof(connectionName) + " not given");

            if (ConfigurationManager.ConnectionStrings[connectionName] == null)
                throw new InvalidOperationException("Connection name not found. A valid connection name is required for the operation.");

            if (_transaction == null)
                throw new InvalidOperationException("No setup found. Use the Setup method to build a new setup then try again.");
            

            _transaction.CommitTransaction(connectionName, credentials);
        }

        /// <summary>
        /// Each transaction requires a valid setup. Examples available at: https://github.com/gtaylor44/SqlBulkTools 
        /// </summary>
        /// <typeparam name="T">The type of collection to be used.</typeparam>
        /// <param name="list"></param>
        /// <returns></returns>
        public CollectionSelect<T> Setup<T>(Func<Setup<T>, CollectionSelect<T>> list)
        {
            CollectionSelect<T> tableSelect = list(new Setup<T>(SourceAlias, TargetAlias, this));
            return tableSelect;
        }
    }
}