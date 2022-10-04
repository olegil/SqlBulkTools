using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SqlBulkTools
{
    /// <summary>
    /// This is infrastructure of transaction
    /// </summary>
    public interface ITransaction
    {
        /// <summary>
        /// Commit transaction when executing bulk operations
        /// </summary>
        /// <param name="connectionName"></param>
        /// <param name="credentials"></param>
        /// <param name="connection"></param>
        void CommitTransaction(string connectionName = null, SqlCredential credentials = null,
            SqlConnection connection = null);

        /// <summary>
        /// Async method when commit transaction when executing bulk operations
        /// </summary>
        /// <param name="connectionName"></param>
        /// <param name="credentials"></param>
        /// <param name="connection"></param>
        /// <returns></returns>
        Task CommitTransactionAsync(string connectionName = null, SqlCredential credentials = null,
            SqlConnection connection = null);
    }
}