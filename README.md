<img src="http://gregnz.com/images/SqlBulkTools/icon-large.png" alt="SqlBulkTools"> 
#SqlBulkTools
-----------------------------
Bulk operations for C# and MSSQL Server with Fluent API. Supports BulkInsert, BulkUpdate, BulkInsertOrUpdate, BulkDelete.

##Examples

####Getting started
-----------------------------

// ISqlBulkTools Interface for easy mocking. <br />

using SqlBulkTools;<br />

public class BookClub(ISqlBulkTools bulk) {<br />

ISqlBulkTools _bulk;<br />
public BookClub(ISqlBulkTools bulk) {<br />
  _bulk = bulk;<br />
}<br />
<br />
}<br />
<br />
Or simply new up an instance if you prefer.<br />
<br />
var bulk = new SqlBulkTools();<br />

###BulkInsert
---------------
List<Books> books = GetBooks();

bulk.Setup(x => x.ForCollection(books))<br />
.WithTable("BooksTable")<br />
.AddColumn(x => x.ISBN)<br />
.AddColumn(x => x.Title)<br />
.AddColumn(x => x.Description)<br />
.BulkInsert();<br />

bulk.CommitTransaction("DefaultConnection");<br />

###BulkInsertOrUpdate
---------------
List<Books> books = GetBooks();

bulk.Setup(x => x.ForCollection(books))<br/>
.WithTable("BooksTable")<br/>
.AddColumn(x => x.ISBN)<br/>
.AddColumn(x => x.Title)<br/>
.AddColumn(x => x.Description)<br/>
.CustomColumnMapping(x => x.Title, "BookTitle") // If SQL column name does not match member name, you can set up a custom mapping. <br/>
.CustomColumnMapping(x => x.Description, "BookDescription")<br/>
.BulkInsertOrUpdate()<br/>
.UpdateOn(x => x.ISBN) // Can add more columns to update on depending on your business rules.<br/>

bulk.CommitTransaction("DefaultConnection");<br/>

- BulkInsertOrUpdate also supports DeleteWhenNotMatched which is false by default. Use at your own risk. 

###BulkUpdate
---------------
List<Books> books = GetBooksToUpdate();<br/>
<br/>
bulk.Setup(x => x.ForCollection(books))<br/>
.WithTable("BooksTable")<br/>
.AddColumn(x => x.ISBN)<br/>
.AddColumn(x => x.Title)<br/>
.AddColumn(x => x.Description)<br/>
.BulkUpdate()<br/>
.UpdateOn(x => x.ISBN) // Can add more columns to update on depending on your business rules.<br/>

bulk.CommitTransaction("DefaultConnection");

###BulkDelete
---------------

// Use a DTO containing only the columns needed for performance gains.

List<BookDto> books = GetBooksIDontLike();<br/>

bulk.Setup(x => x.ForCollection(books))<br/>
.WithTable("BooksTable")<br/>
.AddColumn(x => x.ISBN)<br/>
.BulkUpdate()<br/>
.DeleteOn(x => x.ISBN) // Can add more columns to update on depending on your business rules.<br/>

bulk.CommitTransaction("DefaultConnection");<br/>

###Advanced
---------------
List<Books> books = GetBooks();<br/>

bulk.Setup(x => x.ForCollection(books))<br/>
.WithTable("BooksTable")<br/>
.WithSchema("Api") // Specify a schema <br/>
.WithBulkCopyBatchSize(4000)<br/>
.WithBulkCopyCommandTimeout(720) // Default is 600 seconds<br/>
.WithBulkCopyEnableStreaming(false)<br/>
.WithBulkCopyNotifyAfter(300)<br/>
.WithSqlCommandTimeout(720) // Default is 600 seconds<br/>
.AddColumn(x =>  ........<br/>
