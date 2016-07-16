using System.Data.SqlClient;

namespace SqlBulkTools
{
    public interface ITransaction
    {
        void CommitTransaction(string connectionString, SqlCredential credentials = null);
    }
}
