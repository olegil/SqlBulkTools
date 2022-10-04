using Microsoft.EntityFrameworkCore;
using SqlBulkTools.IntegrationTests.Model;

namespace SqlBulkTools.IntegrationTests;

public class TestContext : DbContext
{
    public TestContext(DbContextOptions options) : base(options)
    {
    }

    public virtual DbSet<Book> Books { get; set; }
    public virtual DbSet<SchemaTest1> SchemaTest1 { get; set; }
    public virtual DbSet<SchemaTest2> SchemaTest2 { get; set; }
    public virtual DbSet<CustomColumnMappingTest> CustomColumnMappingTests { get; set; }
    public virtual DbSet<ReservedColumnNameTest> ReservedColumnNameTests { get; set; }
}