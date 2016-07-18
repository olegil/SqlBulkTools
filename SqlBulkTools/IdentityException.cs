using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkTools
{
    class IdentityException : Exception
    {
        public IdentityException(string message) : base(message + " SQLBulkTools requires the MatchTargetOn overloaded parameter 'isIdentity' " +
                                                            "to be set if an identity column is being used. Please reconfigure your setup and try again.")
        {
        }
    }
}
