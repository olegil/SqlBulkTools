using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace SqlBulkTools.IntegrationTests.Model
{
    [Table("SchemaTest")]
    public class SchemaTest1
    {
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        [Key]
        public int Id { get; set; }

        public string ColumnB { get; set; }
    }
}
