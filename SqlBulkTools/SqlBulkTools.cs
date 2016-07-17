using System;
using System.Data.SqlClient;

namespace SqlBulkTools
{
    public class SqlBulkTools : ITransaction, ISqlBulkTools
    {
        private ITransaction _transaction;
        private const string SourceAlias = "Source";
        private const string TargetAlias = "Target";  

        internal void SetBulkExt(ITransaction transaction)
        {
            _transaction = transaction;
        }

        /// <summary>
        /// Commits a transaction to database. A valid setup must exist for operation to be successful. 
        /// </summary>
        /// <exception cref="connectionString"></exception>
        /// <exception cref="ITransaction"></exception>
        /// <param name="connectionString"></param>
        /// <param name="credentials"></param>
        public void CommitTransaction(string connectionString, SqlCredential credentials = null)
        {
            if (connectionString == null)
                throw new ArgumentNullException(nameof(connectionString) + " not given");

            if (_transaction == null)
                throw new InvalidOperationException("No setup found. Use the Setup method to build a new setup then try again.");
            

            _transaction.CommitTransaction(connectionString, credentials);
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