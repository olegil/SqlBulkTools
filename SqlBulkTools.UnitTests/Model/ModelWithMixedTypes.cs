using System;

namespace SqlBulkTools.UnitTests.Model
{
    public class ModelWithMixedTypes
    {
        public string Title { get; set; }
        public DateTime CreatedTime { get; set; }
        public bool BoolTest { get; set; }
        public int IntegerTest { get; set; }
        public decimal Price { get; set; }

        public ForeignObject ForeignObject { get; set; }
        public object InvalidObject { get; set; }
    }

    public class ForeignObject {}
}
