using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace SqlBulkTools.IntegrationTests.Model
{
    [Table("SchemaTest", Schema = "AnotherSchema")]
    public class SchemaTest2
    {
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        [Key]
        public int Id { get; set; }

        public string ColumnA { get; set; }
    }
}
