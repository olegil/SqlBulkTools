<img src="http://gregnz.com/images/SqlBulkTools/icon-large.png" alt="SqlBulkTools"> 
#SqlBulkTools
-----------------------------
Bulk operations for C# and MSSQL Server with Fluent API. Supports BulkInsert, BulkUpdate, BulkInsertOrUpdate, BulkDelete.

##Examples

####Getting started
-----------------------------
```c#
// ISqlBulkTools Interface for easy mocking.
using SqlBulkTools;

public class BookClub(ISqlBulkTools bulk) {

ISqlBulkTools _bulk;
public BookClub(ISqlBulkTools bulk) {
  _bulk = bulk;
}

}

// Or simply new up an instance if you prefer.
var bulk = new SqlBulkTools();
```
###BulkInsert
---------------
```c#
books = GetBooks();

bulk.Setup(x => x.ForCollection(books))
.WithTable("BooksTable")
.AddColumn(x => x.ISBN)
.AddColumn(x => x.Title)
.AddColumn(x => x.Description)
.BulkInsert();

bulk.CommitTransaction("DefaultConnection");
```
###BulkInsertOrUpdate
---------------
```c#
books = GetBooks();

bulk.Setup(x => x.ForCollection(books))
.WithTable("BooksTable")
.AddColumn(x => x.ISBN)
.AddColumn(x => x.Title)
.AddColumn(x => x.Description)
.CustomColumnMapping(x => x.Title, "BookTitle")
.CustomColumnMapping(x => x.Description, "BookDescription")
.BulkInsertOrUpdate()
.UpdateOn(x => x.ISBN) // Can add more columns to update on depending on your business rules.

bulk.CommitTransaction("DefaultConnection");

/* If SQL column name does not match member name, you can set up a custom mapping. 
For example: "title" does not exist in table but "booktitle" does. This is a use case
to support a custom column mapping (as demonstrated above). 

BulkInsertOrUpdate also supports DeleteWhenNotMatched which is false by default. Use at your own risk.*/
```
###BulkUpdate
---------------
```c#
books = GetBooksToUpdate();

bulk.Setup(x => x.ForCollection(books))
.WithTable("BooksTable")
.AddColumn(x => x.ISBN)
.AddColumn(x => x.Title)
.AddColumn(x => x.Description)
.BulkUpdate()
.UpdateOn(x => x.ISBN) // Can add more columns to update on depending on your business rules.

bulk.CommitTransaction("DefaultConnection");
```
###BulkDelete
---------------
```c#
// Use a DTO containing only the columns needed for performance gains.

books = GetBooksIDontLike();

bulk.Setup(x => x.ForCollection(books))
.WithTable("BooksTable")
.AddColumn(x => x.ISBN)
.BulkDelete()
.DeleteOn(x => x.ISBN) // Can add more columns to update on depending on your business rules.

bulk.CommitTransaction("DefaultConnection");
```
###Advanced
---------------
```c#
books = GetBooks();

bulk.Setup(x => x.ForCollection(books))
.WithTable("BooksTable")
.WithSchema("Api") // Specify a schema 
.WithBulkCopyBatchSize(4000)
.WithBulkCopyCommandTimeout(720) // Default is 600 seconds
.WithBulkCopyEnableStreaming(false)
.WithBulkCopyNotifyAfter(300)
.WithSqlCommandTimeout(720) // Default is 600 seconds<br/>
.AddColumn(x =>  // ........
```
