using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
    }
}
