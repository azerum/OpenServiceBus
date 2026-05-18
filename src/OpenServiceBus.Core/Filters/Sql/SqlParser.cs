namespace OpenServiceBus.Core.Filters.Sql;

/// <summary>
/// Recursive-descent parser for the Service Bus SQL filter subset.
///
/// Grammar (loosely):
///   expression       := or-expr
///   or-expr          := and-expr ( OR and-expr )*
///   and-expr         := unary-expr ( AND unary-expr )*
///   unary-expr       := NOT unary-expr | predicate
///   predicate        := comparison ( IS [NOT] NULL | [NOT] LIKE string | [NOT] IN '(' list ')' )?
///   comparison       := primary ( ( '=' | '!=' | '&lt;&gt;' | '&lt;' | '&lt;=' | '&gt;' | '&gt;=' ) primary )?
///   primary          := literal | property-ref | '(' expression ')' | EXISTS '(' identifier ')' | NOT EXISTS '(' identifier ')'
///   property-ref     := ( identifier '.' )? identifier
/// </summary>
internal sealed class SqlParser
{
    private readonly List<SqlToken> _tokens;
    private int _index;

    public SqlParser(string source)
    {
        _tokens = new SqlLexer(source).Tokenize();
    }

    public SqlExpressionNode ParseExpression()
    {
        var node = ParseOr();
        if (Current.Kind != SqlTokenKind.EndOfInput)
        {
            throw Error($"Unexpected trailing token '{Current.Text}'.");
        }
        return node;
    }

    private SqlExpressionNode ParseOr()
    {
        var left = ParseAnd();
        while (Match(SqlTokenKind.KwOr))
        {
            var right = ParseAnd();
            left = new SqlOrNode(left, right);
        }
        return left;
    }

    private SqlExpressionNode ParseAnd()
    {
        var left = ParseUnary();
        while (Match(SqlTokenKind.KwAnd))
        {
            var right = ParseUnary();
            left = new SqlAndNode(left, right);
        }
        return left;
    }

    private SqlExpressionNode ParseUnary()
    {
        if (Match(SqlTokenKind.KwNot))
        {
            // NOT EXISTS(prop) is a special form (EXISTS takes a property reference).
            if (Current.Kind == SqlTokenKind.KwExists)
            {
                return ParseExistsAfterKeyword(negate: true);
            }
            var inner = ParseUnary();
            return new SqlNotNode(inner);
        }
        return ParsePredicate();
    }

    private SqlExpressionNode ParsePredicate()
    {
        // EXISTS is a primary on its own.
        if (Current.Kind == SqlTokenKind.KwExists)
        {
            return ParseExistsAfterKeyword(negate: false);
        }

        var left = ParseComparison();

        // IS [NOT] NULL
        if (Match(SqlTokenKind.KwIs))
        {
            var negate = Match(SqlTokenKind.KwNot);
            Expect(SqlTokenKind.KwNull, "Expected NULL after IS.");
            return new SqlIsNullNode(left, negate);
        }

        // [NOT] LIKE / [NOT] IN
        var prefixNegate = Match(SqlTokenKind.KwNot);
        if (Match(SqlTokenKind.KwLike))
        {
            var patternToken = Current;
            if (patternToken.Kind != SqlTokenKind.String)
            {
                throw Error("Expected string literal after LIKE.");
            }
            _index++;
            return new SqlLikeNode(left, (string)patternToken.Value!, prefixNegate);
        }
        if (Match(SqlTokenKind.KwIn))
        {
            Expect(SqlTokenKind.LeftParen, "Expected '(' after IN.");
            var values = new List<SqlExpressionNode>();
            while (Current.Kind != SqlTokenKind.RightParen)
            {
                values.Add(ParsePrimary());
                if (!Match(SqlTokenKind.Comma)) break;
            }
            Expect(SqlTokenKind.RightParen, "Expected ')' to close IN list.");
            return new SqlInNode(left, values, prefixNegate);
        }

        if (prefixNegate)
        {
            // The earlier NOT wasn't followed by LIKE/IN; rewind.
            _index--;
            return left;
        }

        return left;
    }

    private SqlExpressionNode ParseComparison()
    {
        var left = ParsePrimary();
        if (TryConsumeComparison(out var op))
        {
            var right = ParsePrimary();
            return new SqlComparisonNode(op, left, right);
        }
        return left;
    }

    private SqlExpressionNode ParsePrimary()
    {
        var token = Current;
        switch (token.Kind)
        {
            case SqlTokenKind.LeftParen:
                _index++;
                var inner = ParseOr();
                Expect(SqlTokenKind.RightParen, "Expected ')'.");
                return inner;

            case SqlTokenKind.Number:
            case SqlTokenKind.String:
            case SqlTokenKind.KwTrue:
            case SqlTokenKind.KwFalse:
                _index++;
                return new SqlLiteralNode(token.Value);

            case SqlTokenKind.KwNull:
                _index++;
                return new SqlLiteralNode(null);

            case SqlTokenKind.Identifier:
                return ParsePropertyRef();
        }
        throw Error($"Unexpected token '{token.Text}'.");
    }

    private SqlExpressionNode ParsePropertyRef()
    {
        var first = Current;
        _index++;
        if (Match(SqlTokenKind.Dot))
        {
            var second = Current;
            if (second.Kind != SqlTokenKind.Identifier)
            {
                throw Error("Expected property name after '.'.");
            }
            _index++;
            return new SqlPropertyRefNode(first.Text, second.Text);
        }
        return new SqlPropertyRefNode(string.Empty, first.Text);
    }

    private SqlExpressionNode ParseExistsAfterKeyword(bool negate)
    {
        Expect(SqlTokenKind.KwExists, "Expected EXISTS.");
        Expect(SqlTokenKind.LeftParen, "Expected '(' after EXISTS.");
        var firstId = Current;
        if (firstId.Kind != SqlTokenKind.Identifier)
        {
            throw Error("Expected property reference inside EXISTS(...).");
        }
        _index++;
        string source = string.Empty;
        string name;
        if (Match(SqlTokenKind.Dot))
        {
            var second = Current;
            if (second.Kind != SqlTokenKind.Identifier)
            {
                throw Error("Expected property name after '.' inside EXISTS.");
            }
            _index++;
            source = firstId.Text;
            name = second.Text;
        }
        else
        {
            name = firstId.Text;
        }
        Expect(SqlTokenKind.RightParen, "Expected ')' to close EXISTS.");
        return new SqlExistsNode(source, name, negate);
    }

    private bool TryConsumeComparison(out SqlComparisonOp op)
    {
        op = default;
        switch (Current.Kind)
        {
            case SqlTokenKind.Eq: op = SqlComparisonOp.Eq; break;
            case SqlTokenKind.NotEq: op = SqlComparisonOp.NotEq; break;
            case SqlTokenKind.Lt: op = SqlComparisonOp.Lt; break;
            case SqlTokenKind.LtEq: op = SqlComparisonOp.LtEq; break;
            case SqlTokenKind.Gt: op = SqlComparisonOp.Gt; break;
            case SqlTokenKind.GtEq: op = SqlComparisonOp.GtEq; break;
            default: return false;
        }
        _index++;
        return true;
    }

    private SqlToken Current => _tokens[_index];

    private bool Match(SqlTokenKind kind)
    {
        if (Current.Kind != kind) return false;
        _index++;
        return true;
    }

    private void Expect(SqlTokenKind kind, string message)
    {
        if (Current.Kind != kind) throw Error(message + $" (got '{Current.Text}').");
        _index++;
    }

    private FormatException Error(string message) =>
        new($"SQL filter parse error near '{Current.Text}' at position {Current.Position}: {message}");
}
