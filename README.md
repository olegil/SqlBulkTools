<img src="http://gregnz.com/images/SqlBulkTools/icon-large.png" alt="SqlBulkTools"> 
#SqlBulkTools
-----------------------------
Bulk operations for C# and MSSQL Server with Fluent API. Supports Bulk Insert, Bulk Update, BulkInsertOrUpdate (Upsert / Merge), Bulk Delete.

##Examples

####Getting started
-----------------------------
```c#
// IBulkOperations Interface for easy mocking.
using SqlBulkTools;

public class BookClub(IBulkOperations bulk) {

  IBulkOperations _bulk;
  
  public BookClub(IBulkOperations bulk) {
    _bulk = bulk;
  }
    // Do your stuff
}

// Or simply new up an instance if you prefer.
var bulk = new BulkOperations();
```
###BulkInsert
---------------
```c#
books = GetBooks();

bulk.Setup<Book>(x => x.ForCollection(books))
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

bulk.Setup<Book>(x => x.ForCollection(books))
.WithTable("BooksTable")
.AddColumn(x => x.ISBN)
.AddColumn(x => x.Title)
.AddColumn(x => x.Description)
.CustomColumnMapping(x => x.Title, "BookTitle")
.CustomColumnMapping(x => x.Description, "BookDescription")
.BulkInsertOrUpdate()
.MatchTargetOn(x => x.ISBN) // Can add more columns to update on depending on your business rules.

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

bulk.Setup<Book>(x => x.ForCollection(books))
.WithTable("BooksTable")
.AddColumn(x => x.ISBN)
.AddColumn(x => x.Title)
.AddColumn(x => x.Description)
.BulkUpdate()
.MatchTargetOn(x => x.ISBN, isIdentity: false) // Can add more columns to update on depending on your business rules.

// Notes: If your column is an auto increment column, then set isIdentity to true. 

bulk.CommitTransaction("DefaultConnection");
```
###BulkDelete
---------------
```c#
// Tip: use a DTO containing only the columns needed for performance gains.

books = GetBooksIDontLike();

bulk.Setup<BookDto>(x => x.ForCollection(books))
.WithTable("BooksTable")
.AddColumn(x => x.ISBN)
.BulkDelete()
.MatchTargetOn(x => x.ISBN) // Can add more columns to update on depending on your business rules.

bulk.CommitTransaction("DefaultConnection");
```
###Advanced
---------------
```c#
books = GetBooks();

bulk.Setup<Book>(x => x.ForCollection(books))
.WithTable("BooksTable")
.WithSchema("Api") // Specify a schema 
.WithBulkCopyBatchSize(4000)
.WithBulkCopyCommandTimeout(720) // Default is 600 seconds
.WithBulkCopyEnableStreaming(false)
.WithBulkCopyNotifyAfter(300)
.WithSqlCommandTimeout(720) // Default is 600 seconds
.AddColumn(x =>  // ........
```
