using System.Data.Entity;
using SqlBulkTools.IntegrationTests.TestModel;

namespace SqlBulkTools.IntegrationTests.TestEnvironment
{
    public class TestContext : DbContext 
    {
        public TestContext()
            : base("SqlBulkToolsTest")
        {
            this.Database.CommandTimeout = 150;
        }

        public virtual DbSet<Book> Books { get; set; }
        public virtual DbSet<SchemaTestCustomSchemaSameName> SchemaTestCustomSchemaConflictingName { get; set; }
        public virtual DbSet<SchemaTestDefaultSchema> SchemaTest { get; set; }
    }
}
