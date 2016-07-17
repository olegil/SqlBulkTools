using System;
using System.Data.SqlClient;

namespace SqlBulkTools
{
    public interface ISqlBulkTools
    {
        void CommitTransaction(string connectionString, SqlCredential credentials = null);
        CollectionSelect<T> Setup<T>(Func<Setup<T>, CollectionSelect<T>> list);
    }
}