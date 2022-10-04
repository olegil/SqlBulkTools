using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace SqlBulkTools
{
    public static class TransactionExtensions
    {
        public static void CommitTransaction(this ITransaction transaction, DbContext context)
        {
            var connection = (SqlConnection)context.Database.GetDbConnection();
            transaction.CommitTransaction(null, null, connection);
        }

        public static async Task CommitTransactionAsync(this ITransaction transaction, DbContext context)
        {
            var connection = (SqlConnection)context.Database.GetDbConnection();
            await transaction.CommitTransactionAsync(null, null, connection);
        }
    }
}