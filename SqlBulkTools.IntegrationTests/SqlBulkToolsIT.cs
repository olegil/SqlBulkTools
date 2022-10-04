using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using AutoFixture;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using NUnit.Framework;
using SqlBulkTools.IntegrationTests.Model;

namespace SqlBulkTools.IntegrationTests;

[TestFixture]
internal class SqlBulkToolsIT
{
    [OneTimeSetUp]
    public void Setup()
    {
        var connectionString = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .Build()
            .GetConnectionString(ConnectionStringName);
        var options = new DbContextOptionsBuilder<TestContext>()
            .UseSqlServer(connectionString,
                builder => { builder.MigrationsAssembly(typeof(SqlBulkToolsIT).Assembly.FullName); })
            .Options;
        _db = new TestContext(options);
        _randomizer = new BookRandomizer();
        _db.Database.EnsureDeleted();
        _db.Database.EnsureCreated();
        _db.Database.Migrate();
        _sqlConnection = (SqlConnection)_db.Database.GetDbConnection();
        DeleteLogFile();
    }

    [OneTimeTearDown]
    public void TearDown()
    {
        _db.Dispose();
        try
        {
            _sqlConnection.Close();
        }
        catch
        {
            //ignored
        }
    }

    private const string ConnectionStringName = "SqlBulkToolsTest";
    private const string LogResultsLocation = @"C:\SqlBulkTools_Log.txt";
    private const int RepeatTimes = 1;

    private BookRandomizer _randomizer;
    private TestContext _db;
    private List<Book> _bookCollection;
    private SqlConnection _sqlConnection;

    [TestCase(1000)]
    public void SqlBulkTools_BulkInsert(int rows)
    {
        BulkDelete(_db.Books.ToList(), _sqlConnection);
        _bookCollection = new List<Book>();
        _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));
        var results = new List<long>();

        AppendToLogFile("Testing BulkInsert with " + rows + " rows");

        for (var i = 0; i < RepeatTimes; i++)
        {
            var time = BulkInsert(_bookCollection, _sqlConnection);
            results.Add(time);
        }

        var avg = results.Average(l => l);
        AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        Assert.AreEqual(rows * RepeatTimes, _db.Books.Count());
    }

    [TestCase(1000)]
    public void SqlBulkTools_BulkInsert_WithAllColumns(int rows)
    {
        BulkDelete(_db.Books.ToList(), _sqlConnection);
        _bookCollection = new List<Book>();
        _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));
        var results = new List<long>();

        AppendToLogFile("Testing BulkInsert with " + rows + " rows");

        for (var i = 0; i < RepeatTimes; i++)
        {
            var time = BulkInsertAllColumns(_bookCollection, _sqlConnection);
            results.Add(time);
        }

        var avg = results.Average(l => l);
        AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        Assert.AreEqual(rows * RepeatTimes, _db.Books.Count());
    }

    [TestCase(500, 500)]
    public void SqlBulkTools_BulkInsertOrUpdate(int rows, int newRows)
    {
        BulkDelete(_db.Books.ToList(), _sqlConnection);
        var fixture = new Fixture();
        _bookCollection = _randomizer.GetRandomCollection(rows);

        var results = new List<long>();

        AppendToLogFile("Testing BulkInsertOrUpdate with " + (rows + newRows) + " rows");

        for (var i = 0; i < RepeatTimes; i++)
        {
            BulkInsert(_bookCollection, _sqlConnection);

            // Update some rows
            for (var j = 0; j < 200; j++)
            {
                var newBook = fixture.Build<Book>().Without(s => s.ISBN).Create();
                var prevIsbn = _bookCollection[j].ISBN;
                _bookCollection[j] = newBook;
                _bookCollection[j].ISBN = prevIsbn;
            }

            // Add new rows
            _bookCollection.AddRange(_randomizer.GetRandomCollection(newRows));


            var time = BulkInsertOrUpdate(_bookCollection, _sqlConnection);
            results.Add(time);

            Assert.AreEqual(rows + newRows, _db.Books.Count());
        }

        var avg = results.Average(l => l);
        AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");
    }


    [TestCase(500, 500)]
    public void SqlBulkTools_BulkInsertOrUpdateAllColumns(int rows, int newRows)
    {
        BulkDelete(_db.Books.ToList(), _sqlConnection);
        var fixture = new Fixture();
        _bookCollection = _randomizer.GetRandomCollection(rows);

        var results = new List<long>();

        AppendToLogFile("Testing BulkInsertOrUpdateAllColumns with " + (rows + newRows) + " rows");

        for (var i = 0; i < RepeatTimes; i++)
        {
            BulkInsert(_bookCollection, _sqlConnection);

            // Update some rows
            for (var j = 0; j < 200; j++)
            {
                var newBook = fixture.Build<Book>().Without(s => s.ISBN).Create();
                var prevIsbn = _bookCollection[j].ISBN;
                _bookCollection[j] = newBook;
                _bookCollection[j].ISBN = prevIsbn;
            }

            // Add new rows
            _bookCollection.AddRange(_randomizer.GetRandomCollection(newRows));


            var time = BulkInsertOrUpdateAllColumns(_bookCollection, _sqlConnection);
            results.Add(time);

            Assert.AreEqual(rows + newRows, _db.Books.Count());
        }

        var avg = results.Average(l => l);
        AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");
    }


    [TestCase(1000)]
    public void SqlBulkTools_BulkUpdate(int rows)
    {
        var fixture = new Fixture();
        fixture.Customizations.Add(new PriceBuilder());
        fixture.Customizations.Add(new IsbnBuilder());
        fixture.Customizations.Add(new TitleBuilder());

        BulkDelete(_db.Books.ToList(), _sqlConnection);

        var results = new List<long>();

        AppendToLogFile("Testing BulkUpdate with " + rows + " rows");

        for (var i = 0; i < RepeatTimes; i++)
        {
            _bookCollection = _randomizer.GetRandomCollection(rows);
            BulkInsert(_bookCollection, _sqlConnection);

            // Update half the rows
            for (var j = 0; j < rows / 2; j++)
            {
                var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
                var prevIsbn = _bookCollection[j].ISBN;
                _bookCollection[j] = newBook;
                _bookCollection[j].ISBN = prevIsbn;
            }

            var time = BulkUpdate(_bookCollection, _sqlConnection);
            results.Add(time);

            var testUpdate = _db.Books.FirstOrDefault();
            Assert.AreEqual(_bookCollection[0].Price, testUpdate.Price);
            Assert.AreEqual(_bookCollection[0].Title, testUpdate.Title);
            Assert.AreEqual(_db.Books.Count(), _bookCollection.Count);

            BulkDelete(_bookCollection, _sqlConnection);
        }

        var avg = results.Average(l => l);
        AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");
    }

    [TestCase(1000)]
    public void SqlBulkTools_BulkDelete(int rows)
    {
        var fixture = new Fixture();
        _bookCollection = _randomizer.GetRandomCollection(rows);
        BulkDelete(_db.Books.ToList(), _sqlConnection);

        var results = new List<long>();

        AppendToLogFile("Testing BulkDelete with " + rows + " rows");

        for (var i = 0; i < RepeatTimes; i++)
        {
            BulkInsert(_bookCollection, _sqlConnection);
            var time = BulkDelete(_bookCollection, _sqlConnection);
            results.Add(time);
            Assert.AreEqual(0, _db.Books.Count());
        }

        var avg = results.Average(l => l);
        AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");
    }

    [TestCase(1000)]
    public async Task SqlBulkTools_BulkInsertAsync(int rows)
    {
        await BulkDeleteAsync(_db.Books.ToList(), _sqlConnection);
        _bookCollection = new List<Book>();
        _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));
        var results = new List<long>();

        AppendToLogFile("Testing BulkInsertAsync with " + rows + " rows");

        for (var i = 0; i < RepeatTimes; i++)
        {
            var time = await BulkInsertAsync(_bookCollection, _sqlConnection);
            results.Add(time);
        }

        var avg = results.Average(l => l);
        AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        Assert.AreEqual(rows * RepeatTimes, _db.Books.Count());
    }

    [TestCase(500, 500)]
    public async Task SqlBulkTools_BulkInsertOrUpdateAsync(int rows, int newRows)
    {
        await BulkDeleteAsync(_db.Books.ToList(), _sqlConnection);
        var fixture = new Fixture();
        _bookCollection = _randomizer.GetRandomCollection(rows);

        var results = new List<long>();

        AppendToLogFile("Testing BulkInsertOrUpdateAsync with " + (rows + newRows) + " rows");

        for (var i = 0; i < RepeatTimes; i++)
        {
            await BulkInsertAsync(_bookCollection, _sqlConnection);

            // Update some rows
            for (var j = 0; j < 200; j++)
            {
                var newBook = fixture.Build<Book>().Without(s => s.ISBN).Create();
                var prevIsbn = _bookCollection[j].ISBN;
                _bookCollection[j] = newBook;
                _bookCollection[j].ISBN = prevIsbn;
            }

            // Add new rows
            _bookCollection.AddRange(_randomizer.GetRandomCollection(newRows));


            var time = await BulkInsertOrUpdateAsync(_bookCollection, _sqlConnection);
            results.Add(time);

            Assert.AreEqual(rows + newRows, _db.Books.Count());
        }

        var avg = results.Average(l => l);
        AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");
    }


    [TestCase(500)]
    [TestCase(1000)]
    public async Task SqlBulkTools_BulkUpdateAsync(int rows)
    {
        var fixture = new Fixture();
        fixture.Customizations.Add(new PriceBuilder());
        fixture.Customizations.Add(new IsbnBuilder());
        fixture.Customizations.Add(new TitleBuilder());

        await BulkDeleteAsync(_db.Books.ToList(), _sqlConnection);

        var results = new List<long>();

        AppendToLogFile("Testing BulkUpdateAsync with " + rows + " rows");

        for (var i = 0; i < RepeatTimes; i++)
        {
            _bookCollection = _randomizer.GetRandomCollection(rows);
            await BulkInsertAsync(_bookCollection, _sqlConnection);

            // Update half the rows
            for (var j = 0; j < rows / 2; j++)
            {
                var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
                var prevIsbn = _bookCollection[j].ISBN;
                _bookCollection[j] = newBook;
                _bookCollection[j].ISBN = prevIsbn;
            }

            var time = await BulkUpdateAsync(_bookCollection, _sqlConnection);
            results.Add(time);

            var testUpdate = await _db.Books.FirstOrDefaultAsync();
            Assert.AreEqual(_bookCollection[0].Price, testUpdate.Price);
            Assert.AreEqual(_bookCollection[0].Title, testUpdate.Title);
            Assert.AreEqual(_db.Books.Count(), _bookCollection.Count);

            await BulkDeleteAsync(_bookCollection, _sqlConnection);
        }

        var avg = results.Average(l => l);
        AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");
    }


    [TestCase(500)]
    [TestCase(1000)]
    public async Task SqlBulkTools_BulkDeleteAsync(int rows)
    {
        var fixture = new Fixture();
        _bookCollection = _randomizer.GetRandomCollection(rows);
        BulkDelete(_db.Books.ToList(), _sqlConnection);

        var results = new List<long>();

        AppendToLogFile("Testing BulkDeleteAsync with " + rows + " rows");

        for (var i = 0; i < RepeatTimes; i++)
        {
            await BulkInsertAsync(_bookCollection, _sqlConnection);
            var time = await BulkDeleteAsync(_bookCollection, _sqlConnection);
            results.Add(time);
            Assert.AreEqual(0, _db.Books.Count());
        }

        var avg = results.Average(l => l);
        AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");
    }

    [Test]
    public void SqlBulkTools_TransactionRollsbackOnError()
    {
        BulkDelete(_db.Books.ToList(), _sqlConnection);

        var fixture = new Fixture();
        fixture.Customizations.Add(new PriceBuilder());
        fixture.Customizations.Add(new IsbnBuilder());
        fixture.Customizations.Add(new TitleBuilder());

        _bookCollection = _randomizer.GetRandomCollection(20);
        BulkInsert(_bookCollection, _sqlConnection);

        var prevBook = _bookCollection[0];

        var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
        var prevIsbn = _bookCollection[0].ISBN;

        // Try to change the first element
        _bookCollection[0] = newBook;
        _bookCollection[0].ISBN = prevIsbn;

        // Force error at element 10. Price is a required field
        _bookCollection.ElementAt(10).Price = null;

        try
        {
            BulkUpdate(_bookCollection, _sqlConnection);
        }
        catch
        {
            // Validate that first element has not changed
            var firstElement = _db.Books.FirstOrDefault();
            Assert.AreEqual(firstElement.Price, prevBook.Price);
            Assert.AreEqual(firstElement.Title, prevBook.Title);
        }
    }

    [Test]
    public void SqlBulkTools_IdentityColumnWhenNotSet_ThrowsIdentityException()
    {
        // Arrange
        BulkDelete(_db.Books, _sqlConnection);
        var bulk = new BulkOperations();
        _bookCollection = _randomizer.GetRandomCollection(20);

        bulk.Setup<Book>(x => x.ForCollection(_bookCollection))
            .WithTable("Books")
            .AddAllColumns()
            .BulkUpdate()
            .MatchTargetOn(x => x.Id);

        // Act & Assert
        Assert.Throws<IdentityException>(() => bulk.CommitTransaction(_sqlConnection));
    }

    [Test]
    public void SqlBulkTools_IdentityColumnSet_UpdatesTargetWhenSetIdentityColumn()
    {
        // Arrange
        BulkDelete(_db.Books, _sqlConnection);
        var bulk = new BulkOperations();
        _bookCollection = _randomizer.GetRandomCollection(20);
        var testDesc = "New Description";

        BulkInsert(_bookCollection, _sqlConnection);

        _bookCollection = _db.Books.ToList();
        _bookCollection.First().Description = testDesc;

        bulk.Setup<Book>(x => x.ForCollection(_bookCollection))
            .WithTable("Books")
            .AddAllColumns()
            .BulkUpdate()
            .SetIdentityColumn(x => x.Id)
            .MatchTargetOn(x => x.Id);

        // Act
        bulk.CommitTransaction(_sqlConnection);

        // Assert
        Assert.AreEqual(testDesc, _db.Books.First().Description);
    }

    [Test]
    public void SqlBulkTools_WithConflictingTableName_DeletesAndInsertsToCorrectTable()
    {
        // Arrange           
        var bulk = new BulkOperations();

        var conflictingSchemaCol = new List<SchemaTest2>();

        for (var i = 0; i < 30; i++)
        {
            conflictingSchemaCol.Add(new SchemaTest2() { ColumnA = "ColumnA " + i });
        }

        // Act            
        bulk.Setup<SchemaTest2>(
                x => x.ForCollection(_db.SchemaTest2.ToList()))
            .WithTable("SchemaTest")
            .WithSchema("AnotherSchema")
            .AddAllColumns()
            .BulkDelete(); // Remove existing rows

        bulk.CommitTransaction(_sqlConnection);

        bulk.Setup<SchemaTest2>(x => x.ForCollection(conflictingSchemaCol))
            .WithTable("SchemaTest")
            .WithSchema("AnotherSchema")
            .AddAllColumns()
            .BulkInsert(); // Add new rows

        bulk.CommitTransaction(_sqlConnection);

        // Assert
        Assert.IsTrue(_db.SchemaTest2.Any());
    }

    [Test]
    public void SqlBulkTools_BulkDeleteOnId_AddItemsThenRemovesAllItems()
    {
        // Arrange           
        var bulk = new BulkOperations();

        var col = new List<SchemaTest1>();

        for (var i = 0; i < 30; i++)
        {
            col.Add(new SchemaTest1() { ColumnB = "ColumnA " + i });
        }

        // Act

        bulk.Setup<SchemaTest1>(x => x.ForCollection(col))
            .WithTable("SchemaTest") // Don't specify schema. Default schema dbo is used. 
            .AddAllColumns()
            .BulkInsert();

        bulk.CommitTransaction(_sqlConnection);

        var allItems = _db.SchemaTest1.ToList();

        bulk.Setup<SchemaTest1>(x => x.ForCollection(allItems))
            .WithTable("SchemaTest")
            .AddColumn(x => x.Id)
            .BulkDelete()
            .MatchTargetOn(x => x.Id);

        bulk.CommitTransaction(_sqlConnection);

        // Assert

        Assert.IsFalse(_db.SchemaTest1.Any());
    }

    [Test]
    public void SqlBulkTools_BulkUpdate_PartialUpdateOnlyUpdatesSelectedColumns()
    {
        // Arrange
        var bulk = new BulkOperations();
        _bookCollection = _randomizer.GetRandomCollection(30);

        BulkDelete(_db.Books.ToList(), _sqlConnection);
        BulkInsert(_bookCollection, _sqlConnection);

        // Update just the price on element 5
        var elemToUpdate = 5;
        decimal updatedPrice = 9999999;
        var originalElement = _bookCollection.ElementAt(elemToUpdate);
        _bookCollection.ElementAt(elemToUpdate).Price = updatedPrice;

        // Act           
        bulk.Setup<Book>(x => x.ForCollection(_bookCollection))
            .WithTable("Books")
            .AddColumn(x => x.Price)
            .BulkUpdate()
            .MatchTargetOn(x => x.ISBN);

        bulk.CommitTransaction(_sqlConnection);

        // Assert
        Assert.AreEqual(updatedPrice, _db.Books.Single(x => x.ISBN == originalElement.ISBN).Price);

        /* Profiler shows: MERGE INTO [SqlBulkTools].[dbo].[Books] WITH (HOLDLOCK) AS Target USING #TmpTable 
         * AS Source ON Target.ISBN = Source.ISBN WHEN MATCHED THEN UPDATE SET Target.Price = Source.Price, 
         * Target.ISBN = Source.ISBN ; DROP TABLE #TmpTable; */
    }

    [Test]
    public void SqlBulkTools_BulkInsertWithColumnMappings_CorrectlyMapsColumns()
    {
        var bulk = new BulkOperations();

        var col = new List<CustomColumnMappingTest>();

        for (var i = 0; i < 30; i++)
        {
            col.Add(new CustomColumnMappingTest()
                { NaturalId = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
        }

        _db.CustomColumnMappingTests.RemoveRange(_db.CustomColumnMappingTests.ToList());
        _db.SaveChanges();

        bulk.Setup<CustomColumnMappingTest>(x => x.ForCollection(col))
            .WithTable("CustomColumnMappingTests")
            .AddAllColumns()
            .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
            .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
            .BulkInsert();

        bulk.CommitTransaction(_sqlConnection);

        // Assert
        Assert.IsTrue(_db.CustomColumnMappingTests.Any());
    }

    [Test]
    public void SqlBulkTools_WhenUsingReservedSqlKeywords()
    {
        _db.ReservedColumnNameTests.RemoveRange(_db.ReservedColumnNameTests.ToList());
        var bulk = new BulkOperations();

        var list = new List<ReservedColumnNameTest>();

        for (var i = 0; i < 30; i++)
        {
            list.Add(new ReservedColumnNameTest() { Key = i });
        }

        bulk.Setup<ReservedColumnNameTest>(x => x.ForCollection(list))
            .WithTable("ReservedColumnNameTests")
            .AddAllColumns()
            .BulkInsertOrUpdate()
            .MatchTargetOn(x => x.Id)
            .SetIdentityColumn(x => x.Id);

        bulk.CommitTransaction(_sqlConnection);

        Assert.IsTrue(_db.ReservedColumnNameTests.Any());
    }

    [Test]
    public void SqlBulkTools_BulkInsertOrUpdate_DecimalValueCorrectlySet()
    {
        _db.Books.RemoveRange(_db.Books.ToList());
        _db.SaveChanges();

        var expectedPrice = (decimal?)1.33;

        var bulk = new BulkOperations();
        var books = new List<Book>()
            { new() { Description = "Test", ISBN = "12345678910", Price = expectedPrice } };

        bulk.Setup<Book>(x => x.ForCollection(books))
            .WithTable("Books")
            .AddAllColumns()
            .BulkInsertOrUpdate()
            .MatchTargetOn(x => x.ISBN)
            .SetIdentityColumn(x => x.Id);

        bulk.CommitTransaction(_sqlConnection);

        Assert.AreEqual(_db.Books.First().Price, expectedPrice);
    }

    [Test]
    public void SqlBulkTools_BulkInsertOrUpdae_FloatValueCorrectlySet()
    {
        _db.Books.RemoveRange(_db.Books.ToList());
        _db.SaveChanges();

        var expectedFloat = (float?)1.33;

        var bulk = new BulkOperations();
        var books = new List<Book>()
            { new() { Description = "Test", ISBN = "12345678910", Price = 30, TestFloat = expectedFloat } };

        bulk.Setup<Book>(x => x.ForCollection(books))
            .WithTable("Books")
            .AddAllColumns()
            .BulkInsertOrUpdate()
            .MatchTargetOn(x => x.ISBN)
            .SetIdentityColumn(x => x.Id);

        bulk.CommitTransaction(_sqlConnection);

        Assert.AreEqual(_db.Books.First().TestFloat, expectedFloat);
    }


    private void AppendToLogFile(string text)
    {
        if (!File.Exists(LogResultsLocation))
        {
            using (var sw = File.CreateText(LogResultsLocation))
            {
                sw.WriteLine(text);
            }

            return;
        }

        using (var sw = File.AppendText(LogResultsLocation))
        {
            sw.WriteLine(text);
        }
    }

    private void DeleteLogFile()
    {
        if (File.Exists(LogResultsLocation))
        {
            File.Delete(LogResultsLocation);
        }
    }

    private long BulkInsert(IEnumerable<Book> col, SqlConnection connection)
    {
        var bulk = new BulkOperations();
        bulk.Setup<Book>(x => x.ForCollection(col))
            .WithTable("Books")
            .WithBulkCopyBatchSize(5000)
            .AddColumn(x => x.Title)
            .AddColumn(x => x.Price)
            .AddColumn(x => x.Description)
            .AddColumn(x => x.ISBN)
            .AddColumn(x => x.PublishDate)
            .TmpDisableAllNonClusteredIndexes()
            .BulkInsert();
        var watch = Stopwatch.StartNew();
        bulk.CommitTransaction(connection);
        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;
        return elapsedMs;
    }

    private long BulkInsertAllColumns(IEnumerable<Book> col, SqlConnection connection)
    {
        var bulk = new BulkOperations();
        bulk.Setup<Book>(x => x.ForCollection(col))
            .WithTable("Books")
            .AddAllColumns()
            .TmpDisableAllNonClusteredIndexes()
            .BulkInsert();
        var watch = Stopwatch.StartNew();
        bulk.CommitTransaction(connection);
        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;
        return elapsedMs;
    }

    private long BulkInsertOrUpdate(IEnumerable<Book> col, SqlConnection connection)
    {
        var bulk = new BulkOperations();
        bulk.Setup<Book>(x => x.ForCollection(col))
            .WithTable("Books")
            .AddColumn(x => x.Title)
            .AddColumn(x => x.Price)
            .AddColumn(x => x.Description)
            .AddColumn(x => x.ISBN)
            .AddColumn(x => x.PublishDate)
            .BulkInsertOrUpdate()
            .MatchTargetOn(x => x.ISBN);

        var watch = Stopwatch.StartNew();
        bulk.CommitTransaction(connection);
        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;

        return elapsedMs;
    }

    private long BulkInsertOrUpdateAllColumns(IEnumerable<Book> col, SqlConnection connection)
    {
        var bulk = new BulkOperations();
        bulk.Setup<Book>(x => x.ForCollection(col))
            .WithTable("Books")
            .AddAllColumns()
            .BulkInsertOrUpdate()
            .SetIdentityColumn(x => x.Id, false)
            .MatchTargetOn(x => x.ISBN);

        var watch = Stopwatch.StartNew();
        bulk.CommitTransaction(connection);
        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;

        return elapsedMs;
    }

    private long BulkUpdate(IEnumerable<Book> col, SqlConnection connection)
    {
        var bulk = new BulkOperations();

        bulk.Setup<Book>(x => x.ForCollection(col))
            .WithTable("Books")
            .AddColumn(x => x.Title)
            .AddColumn(x => x.Price)
            .AddColumn(x => x.Description)
            .AddColumn(x => x.PublishDate)
            .BulkUpdate()
            .MatchTargetOn(x => x.ISBN);

        var watch = Stopwatch.StartNew();
        bulk.CommitTransaction(connection);
        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;

        return elapsedMs;
    }

    private long BulkDelete(IEnumerable<Book> col, SqlConnection sqlConnection)
    {
        var bulk = new BulkOperations();
        bulk.Setup<Book>(x => x.ForCollection(col))
            .WithTable("Books")
            .AddColumn(x => x.ISBN)
            .BulkDelete()
            .MatchTargetOn(x => x.ISBN);

        var watch = Stopwatch.StartNew();
        bulk.CommitTransaction(sqlConnection);
        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;

        return elapsedMs;
    }

    private async Task<long> BulkInsertAsync(IEnumerable<Book> col, SqlConnection connection)
    {
        var bulk = new BulkOperations();
        bulk.Setup<Book>(x => x.ForCollection(col))
            .WithTable("Books")
            .WithSqlBulkCopyOptions(SqlBulkCopyOptions.TableLock)
            .WithBulkCopyBatchSize(3000)
            .AddColumn(x => x.Title)
            .AddColumn(x => x.Price)
            .AddColumn(x => x.Description)
            .AddColumn(x => x.ISBN)
            .AddColumn(x => x.PublishDate)
            .BulkInsert();
        var watch = Stopwatch.StartNew();
        await bulk.CommitTransactionAsync(connection);
        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;
        return elapsedMs;
    }

    private async Task<long> BulkInsertOrUpdateAsync(IEnumerable<Book> col, SqlConnection connection)
    {
        var bulk = new BulkOperations();
        bulk.Setup<Book>(x => x.ForCollection(col))
            .WithTable("Books")
            .AddColumn(x => x.Title)
            .AddColumn(x => x.Price)
            .AddColumn(x => x.Description)
            .AddColumn(x => x.ISBN)
            .AddColumn(x => x.PublishDate)
            .BulkInsertOrUpdate()
            .MatchTargetOn(x => x.ISBN);

        var watch = Stopwatch.StartNew();
        await bulk.CommitTransactionAsync(connection);
        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;

        return elapsedMs;
    }

    private async Task<long> BulkUpdateAsync(IEnumerable<Book> col, SqlConnection connection)
    {
        var bulk = new BulkOperations();
        bulk.Setup<Book>(x => x.ForCollection(col))
            .WithTable("Books")
            .AddColumn(x => x.Title)
            .AddColumn(x => x.Price)
            .AddColumn(x => x.Description)
            .AddColumn(x => x.PublishDate)
            .BulkUpdate()
            .MatchTargetOn(x => x.ISBN);

        var watch = Stopwatch.StartNew();
        await bulk.CommitTransactionAsync(connection);
        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;

        return elapsedMs;
    }

    private async Task<long> BulkDeleteAsync(IEnumerable<Book> col, SqlConnection connection)
    {
        var bulk = new BulkOperations();
        bulk.Setup<Book>(x => x.ForCollection(col))
            .WithTable("Books")
            .AddColumn(x => x.ISBN)
            .BulkDelete()
            .MatchTargetOn(x => x.ISBN);

        var watch = Stopwatch.StartNew();
        await bulk.CommitTransactionAsync(connection);
        watch.Stop();
        var elapsedMs = watch.ElapsedMilliseconds;

        return elapsedMs;
    }
}