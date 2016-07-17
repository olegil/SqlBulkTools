using System.Collections.Generic;
using System.Data.Entity;
using System.IO;
using System.Linq;
using NUnit.Framework;
using Ploeh.AutoFixture;
using SqlBulkTools.IntegrationTests.TestEnvironment;
using SqlBulkTools.IntegrationTests.TestModel;
using TestContext = SqlBulkTools.IntegrationTests.TestEnvironment.TestContext;

namespace SqlBulkTools.IntegrationTests
{
    [TestFixture]
    class SqlBulkToolsTests
    {

        private const string LogResultsLocation = @"C:\SqlBulkTools_Log.txt";
        private const int RepeatTimes = 5;

        private BookRandomizer _randomizer;
        private TestContext _db;
        private List<Book> _bookCollection;
        [OneTimeSetUp]
        public void Setup()
        {
            
            _db = new TestContext();
            _randomizer = new BookRandomizer();
            Database.SetInitializer(new DropCreateDatabaseAlways<TestContext>());
            DeleteLogFile();
            CleanupDatabase();
        }

        [TestCase(500)]
        [TestCase(1000)]
        [TestCase(10000)]
        [TestCase(100000)]
        public void SqlBulkTools_BulkInsert(int rows)
        {
            _bookCollection = new List<Book>();
            _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));
            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkInsert with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                long time = BulkInsert(_bookCollection);
                results.Add(time);
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

            
            CleanupDatabase();
        }

        [TestCase(450, 50)]
        [TestCase(900, 100)]
        public void SqlBulkTools_BulkInsertOrUpdate(int rows, int newRows)
        {
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkInsertOrUpdate with " + (rows + newRows) + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                long time = BulkInsertOrUpdate(_bookCollection);                
                results.Add(time);

                // Update some rows
                for (int j = 0; j < 200; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;
                }

                // Add new rows
                for (int k = 0; k < newRows; k++)
                {
                    _bookCollection.AddRange(_randomizer.GetRandomCollection(newRows));
                }                
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

            CleanupDatabase();

        }

        [TestCase(500)]
        [TestCase(1000)]
        public void SqlBulkTools_BulkUpdate(int rows)
        {
            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());

            _bookCollection = _randomizer.GetRandomCollection(rows);
            BulkInsert(_bookCollection);

            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkUpdate with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                long time = BulkUpdate(_bookCollection);
                results.Add(time);

                // Update half the rows
                for (int j = 0; j < rows / 2; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.Id).Create();
                    var prevId = _bookCollection[j].Id;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].Id = prevId;
                }
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

            CleanupDatabase();

        }

        [TestCase(500)]
        [TestCase(1000)]
        [TestCase(10000)]
        [TestCase(100000)]
        public void SqlBulkTools_BulkDelete(int rows)
        {
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkDelete with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                BulkInsert(_bookCollection);
                long time = BulkDelete(_bookCollection);
                results.Add(time);
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

            CleanupDatabase();

        }


        private void CleanupDatabase()
        {
            int recordCount = _db.Books.Count();

            BookDto[] col = GetBookDtoCol(recordCount);

            BulkOperations bulk = new BulkOperations();
            bulk.Setup<BookDto>(x => x.ForCollection(col))
                .WithTable("Books")
                .AddColumn(x => x.Id)
                .BulkDelete()
                .MatchTargetOn(x => x.Id);
            bulk.CommitTransaction("SqlBulkToolsTest");
        }

        private BookDto[] GetBookDtoCol(int recordCount)
        {
            BookDto[] bookDtoCol = new BookDto[recordCount];

            
            for (int i = 0; i < recordCount; i++)
            {
                // Identity starts from 1 not 0
                bookDtoCol[i] = new BookDto() { Id = i + 1 };
            }

            return bookDtoCol;
        }

        private void AppendToLogFile(string text)
        {
            if (!File.Exists(LogResultsLocation))
            {
                using (StreamWriter sw = File.CreateText(LogResultsLocation))
                {
                    sw.WriteLine(text);
                }

                return;
            }

            using (StreamWriter sw = File.AppendText(LogResultsLocation))
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

        private long BulkInsert(ICollection<Book> col)
        {           
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>(x => x.ForCollection(col))
                .WithTable("Books")
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.PublishDate)
                .BulkInsert();
            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            return elapsedMs;
        }

        private long BulkInsertOrUpdate(ICollection<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>(x => x.ForCollection(col))
                .WithTable("Books")
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.PublishDate)
                .BulkInsertOrUpdate()
                .MatchTargetOn(x => x.ISBN);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private long BulkUpdate(ICollection<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>(x => x.ForCollection(col))
                .WithTable("Books")
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.PublishDate)
                .BulkUpdate()
                .MatchTargetOn(x => x.Id, isIdentity: true);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private long BulkDelete(ICollection<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>(x => x.ForCollection(col))
                .WithTable("Books")
                .AddColumn(x => x.Id)
                .BulkDelete()
                .MatchTargetOn(x => x.Id);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }
    }
}
