using System;
using System.Data.SqlClient;

namespace SqlBulkTools
{
    public class SqlBulkTools : ITransaction, ISqlBulkTools
    {
        private ITransaction _transaction;
        //private readonly BulkExtHelpers _helper;
        private const string SourceAlias = "Source";
        private const string TargetAlias = "Target";  
        //private readonly Dictionary<string, string> _actualColumns;
        //private readonly Dictionary<string, string> _actualColumnsMaxCharLength;

        public SqlBulkTools()
        {
            //_helper = new BulkExtHelpers();
            //_actualColumns = new Dictionary<string, string>();
            //_actualColumnsMaxCharLength = new Dictionary<string, string>();
        }

        internal void SetBulkExt(ITransaction transaction)
        {
            _transaction = transaction;
        }

        public void CommitTransaction(string connectionString, SqlCredential credentials = null)
        {
            if (_transaction == null)
            {
                throw new InvalidOperationException("No setup found. Use the Setup method to build a new setup then try again.");
            }

            _transaction.CommitTransaction(connectionString, credentials);
        }

        public TableSelect<T> Setup<T>(Func<Setup<T>, TableSelect<T>> list)
        {
            TableSelect<T> tableSelect = list(new Setup<T>(SourceAlias, TargetAlias, this));
            return tableSelect;
        }


        //private BulkExtValidation ValidateTableAndColumns(string connectionString, string tableName, string database = null, string schema = null)
        //{
        //    BulkExtValidation model = new BulkExtValidation();
        //    List<string> validationErrors = new List<string>();

        //    model.ValidationErrors = validationErrors;

        //    using (
        //        SqlConnection conn =
        //            new SqlConnection(ConfigurationManager.ConnectionStrings[connectionString].ConnectionString))
        //    {

        //        using (SqlCommand command = new SqlCommand("", conn))
        //        {
        //            try
        //            {
        //                conn.Open();
        //                DataTable tableSchema = conn.GetSchema("Tables");

        //                if (!TableNameAndSchemaValid(tableSchema, validationErrors, tableName, schema, conn.Database))
        //                    return model;


        //                string[] restrictions = new string[4];
        //                restrictions[0] = database ?? conn.Database;
        //                restrictions[1] = schema ?? null;
        //                restrictions[2] = _helper.RemoveSchemaFromTable(tableName);
        //                var dtCols = conn.GetSchema("Columns", restrictions);

        //                foreach (DataRow row in dtCols.Rows)
        //                {
        //                    _actualColumns.Add(row["COLUMN_NAME"].ToString(), row["DATA_TYPE"].ToString());
        //                    _actualColumnsMaxCharLength.Add(row["COLUMN_NAME"].ToString(),
        //                        row["CHARACTER_MAXIMUM_LENGTH"].ToString());
        //                }
        //            }

        //            catch (Exception e)
        //            {
        //                throw;
        //            }
        //        }
        //    }

        //    foreach (var column in Columns.ToList())
        //    {
        //        if (!_actualColumns.ContainsKey(column.Key))
        //        {
        //            validationErrors.Add("Column \"" + column.Key + "\" unable to be matched to table. Check column name and/or schema is correct");

        //        }

        //        else
        //            ValidateTypes(column, validationErrors);

        //    }

        //    if (model.ValidationErrors.Count > 0)
        //    {
        //        model.InError = true;
        //    }

        //    return model;
        //}

        //private bool TableNameAndSchemaValid(DataTable tableSchema, List<string> validationErrors, string tableName, string schema, string databaseName)
        //{
        //    for (int i = 0; i < tableSchema.Rows.Count; i++)
        //    {
        //        if (tableSchema.Rows[i][2].ToString() == tableName)
        //        {
        //            if (schema != null && tableSchema.Rows[i][1].ToString() != schema)
        //            {
        //                continue;
        //            }

        //            return true;
        //        }

        //        if (i == tableSchema.Rows.Count - 1)
        //        {
        //            if (schema != null)
        //            {
        //                validationErrors.Add("Table name under schema " + schema + " could not be found in " + databaseName);
        //                return false;
        //            }
        //            validationErrors.Add("Table name could not be found in " + databaseName);
        //            return false;
        //        }
        //    }

        //    return true;
        //}

        //private void ValidateTypes(KeyValuePair<string, string> column, List<string> validationErrors)
        //{
        //    string actualColumn;

        //    bool exists = _actualColumns.TryGetValue(column.Key, out actualColumn);

        //    if (exists && actualColumn == "varchar" || actualColumn == "nvarchar")
        //    {
        //        Regex reg = new Regex(@"(?<=\().+?(?=\))");
        //        Match match = reg.Match(column.Value);

        //        if (match.Success)
        //        {
        //            string matched = match.Value;

        //            string res;
        //            if (_actualColumnsMaxCharLength.TryGetValue(column.Key, out res))
        //            {
        //                if (res == "-1")
        //                {
        //                    res = "max";
        //                }

        //                if (res != matched)
        //                {
        //                    validationErrors.Add("Column \"" + column.Key + "\" has a type that does not match. Expected " + actualColumn + "(" + res + ")" + " but was " + column.Value);
        //                }
        //            }
        //        }
        //    }

        //    else if (exists && (actualColumn != column.Value))
        //    {
        //        validationErrors.Add("Column \"" + column.Key + "\" has a type that does not match. Expected " + actualColumn + " but was " + column.Value);
        //    }
        //}


    }
}