using System.Globalization;
using System.Text.RegularExpressions;

namespace OpenServiceBus.Core.Filters.Sql;

/// <summary>
/// Three-valued logic helpers used by SQL filter evaluation. <c>NULL</c> propagates through
/// comparisons (NULL = anything → NULL, NOT NULL → NULL), and <c>NULL</c> in a boolean context
/// is treated as false (matches SB's behavior and SQL's WHERE-clause semantics).
/// </summary>
internal static class SqlEvaluator
{
    /// <summary>Coerce an evaluated value to a boolean for top-level WHERE semantics: NULL → false.</summary>
    public static bool AsBool(object? value) => value switch
    {
        null => false,
        bool b => b,
        _ => throw new InvalidOperationException(
            $"SQL filter top-level expression must be boolean; got {value.GetType().Name}: {value}"),
    };

    public static object? CompareEqual(object? left, object? right) =>
        left is null || right is null ? null : Equals(Normalize(left), Normalize(right));

    public static object? CompareNotEqual(object? left, object? right) =>
        CompareEqual(left, right) is bool eq ? !eq : (object?)null;

    public static object? CompareOrdered(object? left, object? right, Func<int, bool> compare)
    {
        if (left is null || right is null) return null;
        var l = Normalize(left);
        var r = Normalize(right);
        if (l is double ld && r is double rd) return compare(ld.CompareTo(rd));
        if (l is long li && r is long ri) return compare(li.CompareTo(ri));
        if (l is long lli && r is double rrd) return compare(((double)lli).CompareTo(rrd));
        if (l is double lld && r is long rri) return compare(lld.CompareTo(rri));
        if (l is string ls && r is string rs) return compare(string.CompareOrdinal(ls, rs));
        if (l is DateTimeOffset ldt && r is DateTimeOffset rdt) return compare(ldt.CompareTo(rdt));
        if (l is bool lb && r is bool rb) return compare(lb.CompareTo(rb));
        return null;
    }

    public static object? LogicalAnd(object? left, object? right)
    {
        // Per SQL NULL semantics: TRUE AND NULL = NULL; FALSE AND anything = FALSE.
        if (left is false || right is false) return false;
        if (left is null || right is null) return null;
        return (bool)left && (bool)right;
    }

    public static object? LogicalOr(object? left, object? right)
    {
        // TRUE OR anything = TRUE; FALSE OR NULL = NULL.
        if (left is true || right is true) return true;
        if (left is null || right is null) return null;
        return (bool)left || (bool)right;
    }

    public static object? LogicalNot(object? value) => value switch
    {
        null => null,
        bool b => !b,
        _ => throw new InvalidOperationException("NOT applied to non-boolean value."),
    };

    public static object? MatchLike(object? value, string pattern)
    {
        if (value is null) return null;
        if (value is not string s) return false;
        var regex = LikeToRegex(pattern);
        return regex.IsMatch(s);
    }

    public static object? MatchIn(object? value, IReadOnlyList<object?> candidates)
    {
        if (value is null) return null;
        foreach (var candidate in candidates)
        {
            if (CompareEqual(value, candidate) is true) return true;
        }
        return false;
    }

    /// <summary>Number unification: ints widen to long, decimals to double, so cross-type compare works.</summary>
    private static object Normalize(object value) => value switch
    {
        int i => (long)i,
        short s => (long)s,
        byte b => (long)b,
        float f => (double)f,
        decimal d => (double)d,
        _ => value,
    };

    private static Regex LikeToRegex(string pattern)
    {
        var sb = new System.Text.StringBuilder("^");
        foreach (var c in pattern)
        {
            switch (c)
            {
                case '%': sb.Append(".*"); break;
                case '_': sb.Append('.'); break;
                default:
                    sb.Append(Regex.Escape(c.ToString(CultureInfo.InvariantCulture)));
                    break;
            }
        }
        sb.Append('$');
        return new Regex(sb.ToString(), RegexOptions.Singleline);
    }
}
