using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Entity.ModelConfiguration.Conventions;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkTools.IntegrationTests.TestModel
{
    public class Book
    {
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        [Key, Column(Order = 0)]
        public int Id { get; set; }

        [MaxLength(13)]
        [Key, Column(Order = 1)]
        public string ISBN { get; set; }

        [MaxLength(256)]
        public string Title { get; set; }

        [MaxLength(2000)]
        public string Description { get; set; }

        public DateTime? PublishDate { get; set; }

        [Required]
        public decimal? Price { get; set; }
    }

}
