<img src="http://gregnz.com/images/SqlBulkTools/icon-large.png" alt="SqlBulkTools"> 
#SqlBulkTools
-----------------------------
Bulk operations for C# and MSSQL Server with Fluent API. Supports BulkInsert, BulkUpdate, BulkInsertOrUpdate, BulkDelete. All operations are highly customizable to cater for your needs. 

##Examples

####Getting started
-----------------------------
- ISqlBulkTools Interface for easy mocking. 

using SqlBulkTools;

public class BookClub(ISqlBulkTools bulk) {

ISqlBulkTools _bulk;
public BookClub(ISqlBulkTools bulk) {
  _bulk = bulk;
}

}

Or simply new up an instance if you prefer.

var bulk = new SqlBulkTools();

###BulkInsert
---------------
List<Books> books = GetBooks();

bulk.Setup(x => x.ForCollection(books))
.WithTable("BooksTable")
.AddColumn(x => x.ISBN)
.AddColumn(x => x.Title)
.AddColumn(x => x.Description)
.BulkInsert();

bulk.CommitTransaction("DefaultConnection");

###BulkInsertOrUpdate
---------------
List<Books> books = GetBooks();

bulk.Setup(x => x.ForCollection(books))
.WithTable("BooksTable")
.AddColumn(x => x.ISBN)
.AddColumn(x => x.Title)
.AddColumn(x => x.Description)
.CustomColumnMapping(x => x.Title, "BookTitle") // If SQL column name does not match member name, you can set up a custom mapping. 
.CustomColumnMapping(x => x.Description, "BookDescription")
.BulkInsertOrUpdate()
.UpdateOn(x => x.ISBN) // Can add more columns to update on depending on your business rules.

bulk.CommitTransaction("DefaultConnection");

- BulkInsertOrUpdate also supports DeleteWhenNotMatched which is false by default. Use at your own risk. 

###BulkUpdate
---------------
List<Books> books = GetBooksToUpdate();

bulk.Setup(x => x.ForCollection(books))
.WithTable("BooksTable")
.AddColumn(x => x.ISBN)
.AddColumn(x => x.Title)
.AddColumn(x => x.Description)
.BulkUpdate()
.UpdateOn(x => x.ISBN) // Can add more columns to update on depending on your business rules.

bulk.CommitTransaction("DefaultConnection");

###BulkDelete
---------------
- Use a DTO containing only the columns needed for performance gains.

List<BookDto> books = GetBooksIDontLike();

bulk.Setup(x => x.ForCollection(books))
.WithTable("BooksTable")
.AddColumn(x => x.ISBN)
.BulkUpdate()
.DeleteOn(x => x.ISBN) // Can add more columns to update on depending on your business rules.

bulk.CommitTransaction("DefaultConnection");

###Advanced
---------------
List<Books> books = GetBooks();

bulk.Setup(x => x.ForCollection(books))
.WithTable("BooksTable")
.WithSchema("Api") // Specify a schema 
.WithBulkCopyBatchSize(4000)
.WithBulkCopyCommandTimeout(720) // Default is 600 seconds
.WithBulkCopyEnableStreaming(false)
.WithBulkCopyNotifyAfter(300)
.WithSqlCommandTimeout(720) // Default is 600 seconds
.AddColumn(x =>  ........
