using System.Collections.Generic;
using NUnit.Framework;

namespace SqlBulkTools.UnitTests
{
    [TestFixture]
    class SqlBulkToolsUnitTests
    {

        [TestCase("[api].[dbo].MyTableName", "MyTableName")]
        [TestCase("[dbo].MyTableName", "MyTableName")]
        [TestCase("MyTableName", "MyTableName")]
        public void BulkExtHelpers_RemoveSchemaFromTable_RemovesSchema(string tableName, string expectedResult)
        {
            // Arrange
            var sut = new BulkOperationsHelpers();

            // Act
            var result = sut.RemoveSchemaFromTable(tableName);

            // Assert
            Assert.AreEqual(expectedResult, result);
        }

        [Test]
        public void BulkExtHelpers_BuildJoinConditionsForUpdateOrInsertWithThreeConditions()
        {
            // Arrange
            List<string> joinOnList = new List<string>() { "MarketPlaceId", "FK_BusinessId", "AddressId" };
            var sut = new BulkOperationsHelpers();

            // Act
            var result = sut.BuildJoinConditionsForUpdateOrInsert(joinOnList.ToArray(), "Source", "Target");

            // Assert
            Assert.AreEqual("ON Target.MarketPlaceId = Source.MarketPlaceId AND Target.FK_BusinessId = Source.FK_BusinessId AND Target.AddressId = Source.AddressId ", result);
        }

        [Test]
        public void BulkExtHelpers_BuildJoinConditionsForUpdateOrInsertWithTwoConditions()
        {
            // Arrange
            List<string> joinOnList = new List<string>() { "MarketPlaceId", "FK_BusinessId" };
            var sut = new BulkOperationsHelpers();

            // Act
            var result = sut.BuildJoinConditionsForUpdateOrInsert(joinOnList.ToArray(), "Source", "Target");

            // Assert
            Assert.AreEqual("ON Target.MarketPlaceId = Source.MarketPlaceId AND Target.FK_BusinessId = Source.FK_BusinessId ", result);
        }

        [Test]
        public void BulkExtHelpers_BuildJoinConditionsForUpdateOrInsertWitSingleCondition()
        {
            // Arrange
            List<string> joinOnList = new List<string>() { "MarketPlaceId" };
            var sut = new BulkOperationsHelpers();

            // Act
            var result = sut.BuildJoinConditionsForUpdateOrInsert(joinOnList.ToArray(), "Source", "Target");

            // Assert
            Assert.AreEqual("ON Target.MarketPlaceId = Source.MarketPlaceId ", result);
        }

        [Test]
        public void BulkExtHelpers_BuildUpdateSet_BuildsCorrectSequenceForMultipleColumns()
        {
            // Arrange
            var updateOrInsertColumns = GetTestParameters();
            var expected =
                "UPDATE SET Target.id = Source.id, Target.Name = Source.Name, Target.Town = Source.Town, Target.Email = Source.Email, Target.IsCool = Source.IsCool ";
            var sut = new BulkOperationsHelpers();

            // Act
            var result = sut.BuildUpdateSet(updateOrInsertColumns, "Source", "Target", null);

            // Assert
            Assert.AreEqual(expected, result);

        }

        [Test]
        public void BulkExtHelpers_BuildUpdateSet_BuildsCorrectSequenceForSingleColumn()
        {
            // Arrange
            var updateOrInsertColumns = new HashSet<string>();
            updateOrInsertColumns.Add("Id");

            var expected =
                "UPDATE SET Target.Id = Source.Id ";
            var sut = new BulkOperationsHelpers();

            // Act
            var result = sut.BuildUpdateSet(updateOrInsertColumns, "Source", "Target", null);

            // Assert
            Assert.AreEqual(expected, result);

        }

        [Test]
        public void BulkExtHelpers_BuildInsertSet_BuildsCorrectSequenceForMultipleColumns()
        {
            // Arrange
            var updateOrInsertColumns = GetTestParameters();
            var expected =
                "INSERT (id, Name, Town, Email, IsCool) values (Source.id, Source.Name, Source.Town, Source.Email, Source.IsCool)";
            var sut = new BulkOperationsHelpers();

            // Act
            var result = sut.BuildInsertSet(updateOrInsertColumns, "Source");

            // Assert
            Assert.AreEqual(expected, result);

        }

        [Test]
        public void BulkExtHelpers_BuildInsertSet_BuildsCorrectSequenceForSingleColumn()
        {
            // Arrange
            var updateOrInsertColumns = new HashSet<string>();
            updateOrInsertColumns.Add("Id");
            var expected =
                "INSERT (Id) values (Source.Id)";
            var sut = new BulkOperationsHelpers();

            // Act
            var result = sut.BuildInsertSet(updateOrInsertColumns, "Source");

            // Assert
            Assert.AreEqual(expected, result);

        }

        private HashSet<string> GetTestParameters()
        {
            HashSet<string> parameters = new HashSet<string>();

            parameters.Add("id");
            parameters.Add("Name");
            parameters.Add("Town");
            parameters.Add("Email");
            parameters.Add("IsCool");

            return parameters;
        } 
    }
}
