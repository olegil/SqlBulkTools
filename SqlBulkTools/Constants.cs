namespace SqlBulkTools
{
    internal static class Constants
    {
        public const string DefaultSchemaName = "dbo";
        public const string InternalId = "SqlBulkTools_InternalId";
        public const string TempTableName = "#TmpTable";
    }

    internal static class IndexOperation
    {
        public const string Rebuild = "REBUILD";
        public const string Disable = "DISABLE";
    }

    #pragma warning disable 1591
    public enum ColumnDirection
    {        
        Input, InputOutput       
    }
    #pragma warning restore 1591

    internal enum OperationType
    {
        Insert, InsertOrUpdate, Update, Delete
    }
}
