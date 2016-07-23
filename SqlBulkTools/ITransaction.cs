using System.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlBulkTools
{
    internal interface ITransaction
    {
        void CommitTransaction(string connectionString, SqlCredential credentials = null);
        Task CommitTransactionAsync(string connectionString, SqlCredential credentials = null);
    }
}
