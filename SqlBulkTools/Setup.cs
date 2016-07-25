using System.Collections.Generic;

namespace SqlBulkTools
{
    public class Setup<T>
    {
        private readonly string _sourceAlias;
        private readonly string _targetAlias;
        private readonly BulkOperations _ext;

        public Setup(string sourceAlias, string targetAlias, BulkOperations ext)
        {
            _sourceAlias = sourceAlias;
            _targetAlias = targetAlias;
            _ext = ext;
        }

        /// <summary>
        /// Represents the collection of objects to be inserted/upserted/updated/deleted (configured in next steps). 
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public CollectionSelect<T> ForCollection(IEnumerable<T> list)
        {
            return new CollectionSelect<T>(list, _sourceAlias, _targetAlias, _ext);
        }
    }
}
