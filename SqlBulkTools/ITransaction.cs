using System.Data.SqlClient;

namespace SqlBulkTools
{
    internal interface ITransaction
    {
        void CommitTransaction(string connectionString, SqlCredential credentials = null);
    }
}
