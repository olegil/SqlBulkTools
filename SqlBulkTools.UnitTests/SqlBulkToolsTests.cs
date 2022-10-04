using System.Collections.Generic;
using NUnit.Framework;
using SqlBulkTools.UnitTests.Model;

namespace SqlBulkTools.UnitTests;

[TestFixture]
internal class SqlBulkToolsUnitTests
{
    [Test]
    public void BulkOperationsHelpers_BuildJoinConditionsForUpdateOrInsertWithThreeConditions()
    {
        // Arrange
        var joinOnList = new List<string>() { "MarketPlaceId", "FK_BusinessId", "AddressId" };
        var sut = new BulkOperationsHelpers();

        // Act
        var result = sut.BuildJoinConditionsForUpdateOrInsert(joinOnList.ToArray(), "Source", "Target");

        // Assert
        Assert.AreEqual(
            "ON [Target].[MarketPlaceId] = [Source].[MarketPlaceId] AND [Target].[FK_BusinessId] = [Source].[FK_BusinessId] AND [Target].[AddressId] = [Source].[AddressId] ",
            result);
    }

    [Test]
    public void BulkOperationsHelpers_BuildJoinConditionsForUpdateOrInsertWithTwoConditions()
    {
        // Arrange
        var joinOnList = new List<string>() { "MarketPlaceId", "FK_BusinessId" };
        var sut = new BulkOperationsHelpers();

        // Act
        var result = sut.BuildJoinConditionsForUpdateOrInsert(joinOnList.ToArray(), "Source", "Target");

        // Assert
        Assert.AreEqual(
            "ON [Target].[MarketPlaceId] = [Source].[MarketPlaceId] AND [Target].[FK_BusinessId] = [Source].[FK_BusinessId] ",
            result);
    }

    [Test]
    public void BulkOperationsHelpers_BuildJoinConditionsForUpdateOrInsertWitSingleCondition()
    {
        // Arrange
        var joinOnList = new List<string>() { "MarketPlaceId" };
        var sut = new BulkOperationsHelpers();

        // Act
        var result = sut.BuildJoinConditionsForUpdateOrInsert(joinOnList.ToArray(), "Source", "Target");

        // Assert
        Assert.AreEqual("ON [Target].[MarketPlaceId] = [Source].[MarketPlaceId] ", result);
    }

    [Test]
    public void BulkOperationsHelpers_BuildUpdateSet_BuildsCorrectSequenceForMultipleColumns()
    {
        // Arrange
        var updateOrInsertColumns = GetTestParameters();
        var expected =
            "UPDATE SET [Target].[id] = [Source].[id], [Target].[Name] = [Source].[Name], [Target].[Town] = [Source].[Town], [Target].[Email] = [Source].[Email], [Target].[IsCool] = [Source].[IsCool] ";
        var sut = new BulkOperationsHelpers();

        // Act
        var result = sut.BuildUpdateSet(updateOrInsertColumns, "Source", "Target", null);

        // Assert
        Assert.AreEqual(expected, result);
    }

    [Test]
    public void BulkOperationsHelpers_BuildUpdateSet_BuildsCorrectSequenceForSingleColumn()
    {
        // Arrange
        var updateOrInsertColumns = new HashSet<string>();
        updateOrInsertColumns.Add("Id");

        var expected =
            "UPDATE SET [Target].[Id] = [Source].[Id] ";
        var sut = new BulkOperationsHelpers();

        // Act
        var result = sut.BuildUpdateSet(updateOrInsertColumns, "Source", "Target", null);

        // Assert
        Assert.AreEqual(expected, result);
    }

    [Test]
    public void BulkOperationsHelpers_BuildInsertSet_BuildsCorrectSequenceForMultipleColumns()
    {
        // Arrange
        var updateOrInsertColumns = GetTestParameters();
        var expected =
            "INSERT ([Name], [Town], [Email], [IsCool]) values ([Source].[Name], [Source].[Town], [Source].[Email], [Source].[IsCool])";
        var sut = new BulkOperationsHelpers();

        // Act
        var result = sut.BuildInsertSet(updateOrInsertColumns, "Source", "id");

        // Assert
        Assert.AreEqual(expected, result);
    }

    [Test]
    public void BulkOperationsHelpers_BuildInsertSet_BuildsCorrectSequenceForSingleColumn()
    {
        // Arrange
        var updateOrInsertColumns = new HashSet<string>();
        updateOrInsertColumns.Add("Id");
        var expected =
            "INSERT ([Id]) values ([Source].[Id])";
        var sut = new BulkOperationsHelpers();

        // Act
        var result = sut.BuildInsertSet(updateOrInsertColumns, "Source", null);

        // Assert
        Assert.AreEqual(expected, result);
    }

    [Test]
    public void BulkOperationsHelpers_GetAllValueTypeAndStringColumns_ReturnsCorrectSet()
    {
        // Arrange
        var helper = new BulkOperationsHelpers();
        var expected = new HashSet<string>() { "Title", "CreatedTime", "BoolTest", "IntegerTest", "Price" };

        // Act
        var result = helper.GetAllValueTypeAndStringColumns(typeof(ModelWithMixedTypes));

        // Assert
        CollectionAssert.AreEqual(expected, result);
    }

    [Test]
    public void BuilOperationsHelpers_GetIndexManagementCmd_WhenDisableAllIndexesIsTrueReturnsCorrectCmd()
    {
        // Arrange
        var expected =
            @"DECLARE @sql AS VARCHAR(MAX)=''; SELECT @sql = @sql + 'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' DISABLE;'FROM sys.indexes JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id WHERE sys.indexes.type_desc = 'NONCLUSTERED' AND sys.objects.type_desc = 'USER_TABLE' AND sys.objects.name = 'Books'; EXEC(@sql);";
        var helper = new BulkOperationsHelpers();

        // Act
        var result = helper.GetIndexManagementCmd(IndexOperation.Disable, "Books", null, true);

        // Assert
        Assert.AreEqual(expected, result);
    }

    [Test]
    public void BuilOperationsHelpers_GetIndexManagementCmd_WithOneIndexReturnsCorrectCmd()
    {
        // Arrange
        var expected =
            @"DECLARE @sql AS VARCHAR(MAX)=''; SELECT @sql = @sql + 'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' DISABLE;'FROM sys.indexes JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id WHERE sys.indexes.type_desc = 'NONCLUSTERED' AND sys.objects.type_desc = 'USER_TABLE' AND sys.objects.name = 'Books' AND sys.indexes.name = 'IX_Title'; EXEC(@sql);";
        var helper = new BulkOperationsHelpers();
        var indexes = new HashSet<string>();
        indexes.Add("IX_Title");

        // Act
        var result = helper.GetIndexManagementCmd(IndexOperation.Disable, "Books", indexes);

        // Assert
        Assert.AreEqual(expected, result);
    }

    [Test]
    public void BuildOperationsHelpers_RebuildSchema_WithExplicitSchemaIsCorrect()
    {
        // Arrange
        var expected = "[db].[CustomSchemaName].[TableName]";
        var helper = new BulkOperationsHelpers();

        // Act
        var result = helper.GetFullQualifyingTableName("db", "CustomSchemaName", "TableName");

        // Act
        Assert.AreEqual(expected, result);
    }

    [Test]
    public void BuilOperationsHelpers_GetIndexManagementCmd_WithListOfIndexesReturnsCorrectCmd()
    {
        // Arrange
        var expected =
            @"DECLARE @sql AS VARCHAR(MAX)=''; SELECT @sql = @sql + 'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' DISABLE;'FROM sys.indexes JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id WHERE sys.indexes.type_desc = 'NONCLUSTERED' AND sys.objects.type_desc = 'USER_TABLE' AND sys.objects.name = 'Books' AND sys.indexes.name = 'IX_Title' AND sys.indexes.name = 'IX_Price'; EXEC(@sql);";
        var helper = new BulkOperationsHelpers();
        var indexes = new HashSet<string>();
        indexes.Add("IX_Title");
        indexes.Add("IX_Price");

        // Act
        var result = helper.GetIndexManagementCmd(IndexOperation.Disable, "Books", indexes);

        // Assert
        Assert.AreEqual(expected, result);
    }


    private HashSet<string> GetTestParameters()
    {
        var parameters = new HashSet<string>();

        parameters.Add("id");
        parameters.Add("Name");
        parameters.Add("Town");
        parameters.Add("Email");
        parameters.Add("IsCool");

        return parameters;
    }
}