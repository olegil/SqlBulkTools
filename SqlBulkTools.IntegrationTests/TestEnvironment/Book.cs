using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkTools.IntegrationTests.TestModel
{
    public class Book
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        [MaxLength(13)]
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
