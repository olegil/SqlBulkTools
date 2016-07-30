using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkTools.IntegrationTests.Model
{
    public class CustomColumnMappingTest
    {
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        [Key]
        public int NaturalId { get; set; }

        [Column("ColumnX"), StringLength(256)]
        public string ColumnXIsDifferent { get; set; }

        [Column("ColumnY")]
        public int ColumnYIsDifferentInDatabase { get; set; }
    }
}
