using System;
using System.Data.Entity;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SqlBulkTools
{
    public static class TransactionExtensions
    {
        public static void CommitTransaction(this ITransaction transaction, DbContext context)
        {
            var connection = (SqlConnection)context.Database.Connection;
            transaction.CommitTransaction(null, null, connection);
        }

        public static async Task CommitTransactionAsync(this ITransaction transaction, DbContext context)
        {
            var connection = (SqlConnection)context.Database.Connection;
            await transaction.CommitTransactionAsync(null, null, connection);
        }
    }
}