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
        private const int RepeatTimes = 1;

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
        public void SqlBulkTools_BulkInsert(int rows)
        {
            BulkDelete(_db.Books.ToList());
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

            Assert.AreEqual(rows * RepeatTimes, _db.Books.Count());

            CleanupDatabase();
        }

        [TestCase(450, 50)]
        [TestCase(900, 100)]
        public void SqlBulkTools_BulkInsertOrUpdate(int rows, int newRows)
        {
            BulkDelete(_db.Books.ToList());
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkInsertOrUpdate with " + (rows + newRows) + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                CleanupDatabase();
                BulkInsert(_bookCollection);

                // Update some rows
                for (int j = 0; j < 200; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;
                }

                // Add new rows
                _bookCollection.AddRange(_randomizer.GetRandomCollection(newRows));
                

                long time = BulkInsertOrUpdate(_bookCollection);
                results.Add(time);

                Assert.AreEqual(rows + newRows, _db.Books.Count());

            }

            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

                      
        }

        [TestCase(500)]
        [TestCase(1000)]
        [TestCase(2000)]
        public void SqlBulkTools_BulkUpdate(int rows)
        {
            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());

            BulkDelete(_db.Books.ToList());

            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkUpdate with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                
                _bookCollection = _randomizer.GetRandomCollection(rows);
                BulkInsert(_bookCollection);

                // Update half the rows
                for (int j = 0; j < rows / 2; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;

                }

                long time = BulkUpdate(_bookCollection);
                results.Add(time);

                var testUpdate = _db.Books.FirstOrDefault();
                Assert.AreEqual(_bookCollection[0].Price, testUpdate.Price);
                Assert.AreEqual(_bookCollection[0].Title, testUpdate.Title);
                Assert.AreEqual(_db.Books.Count(), _bookCollection.Count);

                BulkDelete(_bookCollection);
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }

        [TestCase(500)]
        [TestCase(1000)]
        [TestCase(10000)]
        public void SqlBulkTools_BulkDelete(int rows)
        {
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);
            CleanupDatabase();

            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkDelete with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                BulkInsert(_bookCollection);
                long time = BulkDelete(_bookCollection);
                results.Add(time);
                Assert.AreEqual(0, _db.Books.Count());
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

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
                .AddColumn(x => x.PublishDate)
                .BulkUpdate()
                .MatchTargetOn(x => x.ISBN);

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
                .AddColumn(x => x.ISBN)
                .BulkDelete()
                .MatchTargetOn(x => x.ISBN);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }
    }
}
