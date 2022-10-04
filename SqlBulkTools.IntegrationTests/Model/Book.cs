using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace SqlBulkTools.IntegrationTests.Model;

[Index("ISBN", "Title", "Price")]
public class Book
{
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    [Key]
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

    public float? TestFloat { get; set; }
}