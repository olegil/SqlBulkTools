using System.Collections.Generic;
using System.Data.Entity;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Ploeh.AutoFixture;
using SqlBulkTools.IntegrationTests.TestEnvironment;
using SqlBulkTools.IntegrationTests.TestModel;
using TestContext = SqlBulkTools.IntegrationTests.TestEnvironment.TestContext;

namespace SqlBulkTools.IntegrationTests
{
    [TestFixture]
    class SqlBulkToolsIT
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
        }

        [TestCase(500)]
        [TestCase(1000)]
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
        }

        [TestCase(500)]
        [TestCase(1000)]
        public void SqlBulkTools_BulkInsert_WithAllColumns(int rows)
        {
            BulkDelete(_db.Books.ToList());
            _bookCollection = new List<Book>();
            _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));
            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkInsert with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                long time = BulkInsertAllColumns(_bookCollection);
                results.Add(time);
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

            Assert.AreEqual(rows * RepeatTimes, _db.Books.Count());
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
                Assert.AreEqual(0, _db.Books.Count());
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }

        [TestCase(500)]
        [TestCase(1000)]
        public async Task SqlBulkTools_BulkInsertAsync(int rows)
        {
            await BulkDeleteAsync(_db.Books.ToList());
            _bookCollection = new List<Book>();
            _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));
            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkInsertAsync with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                long time = await BulkInsertAsync(_bookCollection);
                results.Add(time);
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

            Assert.AreEqual(rows * RepeatTimes, _db.Books.Count());
        }

        [TestCase(450, 50)]
        [TestCase(900, 100)]
        public async Task SqlBulkTools_BulkInsertOrUpdateAsync(int rows, int newRows)
        {
            await BulkDeleteAsync(_db.Books.ToList());
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkInsertOrUpdateAsync with " + (rows + newRows) + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                await BulkInsertAsync(_bookCollection);

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


                long time = await BulkInsertOrUpdateAsync(_bookCollection);
                results.Add(time);

                Assert.AreEqual(rows + newRows, _db.Books.Count());

            }

            double avg = results.Average(l => l);
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

            await BulkDeleteAsync(_db.Books.ToList());

            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkUpdateAsync with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {

                _bookCollection = _randomizer.GetRandomCollection(rows);
                await BulkInsertAsync(_bookCollection);

                // Update half the rows
                for (int j = 0; j < rows / 2; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;

                }

                long time = await BulkUpdateAsync(_bookCollection);
                results.Add(time);

                var testUpdate = await _db.Books.FirstOrDefaultAsync();
                Assert.AreEqual(_bookCollection[0].Price, testUpdate.Price);
                Assert.AreEqual(_bookCollection[0].Title, testUpdate.Title);
                Assert.AreEqual(_db.Books.Count(), _bookCollection.Count);

                await BulkDeleteAsync(_bookCollection);
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }


        [TestCase(500)]
        [TestCase(1000)]
        public async Task SqlBulkTools_BulkDeleteAsync(int rows)
        {
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkDeleteAsync with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                await BulkInsertAsync(_bookCollection);
                long time = await BulkDeleteAsync(_bookCollection);
                results.Add(time);
                Assert.AreEqual(0, _db.Books.Count());
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }

        [Test]
        public void SqlBulkTools_TransactionRollsbackOnError()
        {
            BulkDelete(_db.Books.ToList());

            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());

            _bookCollection = _randomizer.GetRandomCollection(20);
            BulkInsert(_bookCollection);

            var prevBook = _bookCollection[0];

            var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
            var prevIsbn = _bookCollection[0].ISBN;
            _bookCollection[0] = newBook;
            _bookCollection[0].ISBN = prevIsbn;

            _bookCollection.ElementAt(10).Price = null; // Price is required

            try
            {
                BulkUpdate(_bookCollection);
            }
            catch
            {
                var firstElement = _db.Books.FirstOrDefault();
                Assert.AreEqual(firstElement.Price, prevBook.Price);
                Assert.AreEqual(firstElement.Title, prevBook.Title);
            }
        }

        [Test]
        public void SqlBulkTools_IdentityColumnWhenNotSet_ThrowsIdentityException()
        {
            // Arrange
            BulkDelete(_db.Books);
            BulkOperations bulk = new BulkOperations();
            _bookCollection = _randomizer.GetRandomCollection(20);

            bulk.Setup<Book>(x => x.ForCollection(_bookCollection))
                .WithTable("Books")
                .AddAllColumns()
                .BulkUpdate()
                .MatchTargetOn(x => x.Id);

            // Act & Assert
            Assert.Throws<IdentityException>(() => bulk.CommitTransaction("SqlBulkToolsTest"));

        }

        [Test]
        public void SqlBulkTools_IdentityColumnSet_UpdatesTarget()
        {
            // Arrange
            BulkDelete(_db.Books);
            BulkOperations bulk = new BulkOperations();
            _bookCollection = _randomizer.GetRandomCollection(20);
            string testDesc = "New Description";

            BulkInsert(_bookCollection);

            _bookCollection = _db.Books.ToList();
            _bookCollection.First().Description = testDesc;

            bulk.Setup<Book>(x => x.ForCollection(_bookCollection))
                .WithTable("Books")
                .AddAllColumns()
                .BulkUpdate()
                .MatchTargetOn(x => x.Id, true);

            // Act
            bulk.CommitTransaction("SqlBulkToolsTest");
            
            // Assert
            Assert.AreEqual(testDesc, _db.Books.First().Description);
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

        private long BulkInsert(IEnumerable<Book> col)
        {           
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>(x => x.ForCollection(col))
                .WithTable("Books")
                .WithBulkCopyBatchSize(3000)
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.PublishDate)
                .BulkInsert();
            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction(new SqlConnection("Data Source=DESKTOP-6I9FL7M;Initial Catalog=SqlBulkTools;Integrated Security=True;Pooling=false"));
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            return elapsedMs;
        }

        private long BulkInsertAllColumns(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>(x => x.ForCollection(col))
                .WithTable("Books")
                .WithBulkCopyBatchSize(3000)
                .AddAllColumns()
                .BulkInsert();
            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction(new SqlConnection("Data Source=DESKTOP-6I9FL7M;Initial Catalog=SqlBulkTools;Integrated Security=True;Pooling=false"));
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            return elapsedMs;
        }

        private long BulkInsertOrUpdate(IEnumerable<Book> col)
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

        private long BulkUpdate(IEnumerable<Book> col)
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

        private long BulkDelete(IEnumerable<Book> col)
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

        private async Task<long> BulkInsertAsync(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>(x => x.ForCollection(col))
                .WithTable("Books")
                .WithBulkCopyBatchSize(3000)
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.PublishDate)
                .BulkInsert();
            var watch = System.Diagnostics.Stopwatch.StartNew();
            await bulk.CommitTransactionAsync("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            return elapsedMs;
        }

        private async Task<long> BulkInsertOrUpdateAsync(IEnumerable<Book> col)
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
            await bulk.CommitTransactionAsync("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private async Task<long> BulkUpdateAsync(IEnumerable<Book> col)
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
            await bulk.CommitTransactionAsync("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private async Task<long> BulkDeleteAsync(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>(x => x.ForCollection(col))
                .WithTable("Books")
                .AddColumn(x => x.ISBN)
                .BulkDelete()
                .MatchTargetOn(x => x.ISBN);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            await bulk.CommitTransactionAsync("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

    }
}
