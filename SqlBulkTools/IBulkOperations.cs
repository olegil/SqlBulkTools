using System;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public interface IBulkOperations
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        void CommitTransaction(SqlConnection connection);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        Task CommitTransactionAsync(SqlConnection connection);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionName"></param>
        /// <param name="credentials"></param>
        void CommitTransaction(string connectionName, SqlCredential credentials = null);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="connectionName"></param>
        /// <param name="credentials"></param>
        /// <returns></returns>
        Task CommitTransactionAsync(string connectionName, SqlCredential credentials = null);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        CollectionSelect<T> Setup<T>(Func<Setup<T>, CollectionSelect<T>> list);
    }
}