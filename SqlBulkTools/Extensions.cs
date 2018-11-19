using System.Collections.Generic;

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
    }
}
