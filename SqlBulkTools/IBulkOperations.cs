using System;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlBulkTools
{
    public interface IBulkOperations
    {
        void CommitTransaction(string connectionName, SqlCredential credentials = null);
        Task CommitTransactionAsync(string connectionName, SqlCredential credentials = null);
        CollectionSelect<T> Setup<T>(Func<Setup<T>, CollectionSelect<T>> list);
    }
}