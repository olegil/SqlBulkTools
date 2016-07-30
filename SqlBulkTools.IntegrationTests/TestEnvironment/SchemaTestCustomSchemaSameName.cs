using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkTools.IntegrationTests.TestEnvironment
{
    [Table("SchemaTest", Schema = "AnotherSchema")]
    public class SchemaTestCustomSchemaSameName
    {
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        [Key]
        public int Id { get; set; }

        public string ColumnA { get; set; }
    }
}
