using System.Data.SqlClient;
using System.Threading.Tasks;

namespace AgentFire.Sql.BulkTools
{
    internal interface ITransaction : IFluentSyntax
    {
        void CommitTransaction(string connectionName = null, SqlCredential credentials = null, SqlConnection connection = null);
        Task CommitTransactionAsync(string connectionName = null, SqlCredential credentials = null, SqlConnection connection = null);
    }
}
