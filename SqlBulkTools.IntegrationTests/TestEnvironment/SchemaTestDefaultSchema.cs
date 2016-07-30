using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace SqlBulkTools.IntegrationTests.TestEnvironment
{
    [Table("SchemaTest")]
    public class SchemaTestDefaultSchema
    {
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        [Key]
        public int Id { get; set; }

        public string ColumnB { get; set; }
    }
}
