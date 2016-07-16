using System.Collections.Generic;

namespace SqlBulkTools
{
    public class BulkExtValidation
    {
        public bool InError { get; set; }
        public List<string> ValidationErrors { get; set; } 
    }
}