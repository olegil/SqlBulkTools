using System;
using System.Data.SqlClient;

namespace SqlBulkTools
{
    public interface ISqlBulkTools
    {
        void CommitTransaction(string connectionString, SqlCredential credentials = null);
        TableSelect<T> Setup<T>(Func<Setup<T>, TableSelect<T>> list);
    }
}