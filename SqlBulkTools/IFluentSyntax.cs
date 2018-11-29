using System;
using System.ComponentModel;

namespace AgentFire.Sql.BulkTools
{
    /// <summary>
    /// A hack to hide methods defined on <see cref="object"/> for IntelliSense
    /// on fluent interfaces. Credit to Daniel Cazzulino.
    /// </summary>
    /// <remarks>
    /// Credit to https://github.com/ninject/Ninject/blob/master/src/Ninject/Syntax/IFluentSyntax.cs
    /// </remarks>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public interface IFluentSyntax
    {
        /// <summary>
        /// Gets the type of this instance.
        /// </summary>
        /// <returns>The type of this instance.</returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        Type GetType();

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table.
        /// </returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        int GetHashCode();

        /// <summary>
        /// Returns a <see cref="string"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string"/> that represents this instance.
        /// </returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        string ToString();

        /// <summary>
        /// Determines whether the specified <see cref="object"/> is equal to this instance.
        /// </summary>
        /// <param name="other">The <see cref="object"/> to compare with this instance.</param>
        /// <returns>
        ///     <c>true</c> if the specified <see cref="object"/> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        [EditorBrowsable(EditorBrowsableState.Never)]
        bool Equals(object other);
    }
}
