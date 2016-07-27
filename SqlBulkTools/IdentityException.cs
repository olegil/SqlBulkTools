using System;

namespace SqlBulkTools
{
    internal class IdentityException : Exception
    {
        public IdentityException(string message) : base(message + " SQLBulkTools requires the MatchTargetOn parameter 'isIdentity' " +
                                                            "to be set to true if an identity column is being used. Please reconfigure your setup and try again.")
        {
        }
    }
}
