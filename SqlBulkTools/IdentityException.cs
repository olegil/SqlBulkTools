using System;

namespace SqlBulkTools
{
    internal class IdentityException : Exception
    {
        public IdentityException(string message) : base(message + " SQLBulkTools requires the SetIdentityColumn method " +
                                                            "to be configured if an identity column is being used. Please reconfigure your setup and try again.")
        {
        }
    }
}
