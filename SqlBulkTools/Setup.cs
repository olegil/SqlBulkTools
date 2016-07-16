using System.Collections.Generic;

namespace SqlBulkTools
{
    public class Setup<T>
    {
        private readonly string _sourceAlias;
        private readonly string _targetAlias;
        private readonly SqlBulkTools _ext;

        internal Setup(string sourceAlias, string targetAlias, SqlBulkTools ext)
        {
            _sourceAlias = sourceAlias;
            _targetAlias = targetAlias;
            _ext = ext;
        }

        public TableSelect<T> ForCollection(List<T> list)
        {
            return new TableSelect<T>(list, _sourceAlias, _targetAlias, _ext);
        }


    }
}
