using System.Collections.Generic;
using System.Data.Linq;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace AgentFire.Sql.BulkTools
{
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
    public static class Extensions
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
    {
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
        public static CollectionSelect<T> Setup<T>(this IBulkOperations op, IEnumerable<T> collection)
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
        {
            return op.Setup<T>(s => s.ForCollection(collection));
        }

        /// <summary>
        /// Commits a transaction to database using the specified Linq-to-Sql's <see cref="DataContext"/> type for the <see cref="SqlConnection"/> property.
        /// </summary>
        public static void CommitTransaction<T>(this BulkOperations op) where T : DataContext, new()
        {
            using (T db = new T())
            {
                op.CommitTransaction((SqlConnection)db.Connection);
            }
        }

        /// <summary>
        /// Asynchronously commits a transaction to database using the specified Linq-to-Sql's <see cref="DataContext"/> type for the <see cref="SqlConnection"/> property.
        /// </summary>
        public static async Task CommitTransactionAsync<T>(this BulkOperations op) where T : DataContext, new()
        {
            using (T db = new T())
            {
                await op.CommitTransactionAsync((SqlConnection)db.Connection).ConfigureAwait(false);
            }
        }
    }
}
