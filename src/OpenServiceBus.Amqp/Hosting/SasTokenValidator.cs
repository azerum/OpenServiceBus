using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Text;

namespace OpenServiceBus.Amqp.Hosting;

/// <summary>
/// Validates Service Bus SAS tokens that arrive at <c>$cbs put-token</c>. Reverse of
/// <c>Azure.Messaging.ServiceBus.Authorization.SharedAccessSignature.BuildSignature</c>:
///
///   <c>stringToSign = urlEncode(audience) + "\n" + expiration</c><br/>
///   <c>signature    = base64(HMAC-SHA256(stringToSign, UTF8(key)))</c><br/>
///   <c>token        = "SharedAccessSignature sr={audience}&amp;sig={urlEncode(sig)}&amp;se={urlEncode(expiry)}&amp;skn={urlEncode(keyName)}"</c>
///
/// We never re-encode the audience: the value of <c>sr</c> is already what the signer used.
/// </summary>
public static class SasTokenValidator
{
    private const string Prefix = "SharedAccessSignature ";

    public static SasValidationResult Validate(
        string? sasToken,
        IReadOnlyDictionary<string, string> keysByName,
        DateTimeOffset now)
    {
        if (string.IsNullOrWhiteSpace(sasToken))
        {
            return SasValidationResult.Invalid("Missing token");
        }
        if (!sasToken.StartsWith(Prefix, StringComparison.Ordinal))
        {
            return SasValidationResult.Invalid("Token does not start with 'SharedAccessSignature '");
        }

        var fields = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in sasToken[Prefix.Length..].Split('&'))
        {
            var eq = pair.IndexOf('=');
            if (eq < 0) return SasValidationResult.Invalid($"Malformed key=value pair: {pair}");
            fields[pair[..eq]] = pair[(eq + 1)..];
        }

        if (!fields.TryGetValue("sr", out var srEncoded)) return SasValidationResult.Invalid("Missing 'sr'");
        if (!fields.TryGetValue("sig", out var sigEncoded)) return SasValidationResult.Invalid("Missing 'sig'");
        if (!fields.TryGetValue("se", out var seEncoded)) return SasValidationResult.Invalid("Missing 'se'");
        if (!fields.TryGetValue("skn", out var sknEncoded)) return SasValidationResult.Invalid("Missing 'skn'");

        var seDecoded = WebUtility.UrlDecode(seEncoded);
        if (!long.TryParse(seDecoded, NumberStyles.Integer, CultureInfo.InvariantCulture, out var expiryUnix))
        {
            return SasValidationResult.Invalid("'se' is not an integer");
        }
        var expiry = DateTimeOffset.FromUnixTimeSeconds(expiryUnix);
        if (expiry <= now)
        {
            return SasValidationResult.Invalid($"Token expired at {expiry:O}");
        }

        var keyName = WebUtility.UrlDecode(sknEncoded);
        if (!keysByName.TryGetValue(keyName, out var key))
        {
            return SasValidationResult.Invalid($"Unknown key name '{keyName}'");
        }

        // Recompute the signature using the same input the signer used: `sr` value as-is + "\n" + expiry-as-integer-string.
        var stringToSign = $"{srEncoded}\n{seDecoded}";
        string expectedSig;
        using (var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(key)))
        {
            expectedSig = Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(stringToSign)));
        }

        var actualSig = WebUtility.UrlDecode(sigEncoded);
        if (!CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(expectedSig), Encoding.UTF8.GetBytes(actualSig)))
        {
            return SasValidationResult.Invalid("Signature mismatch");
        }

        return SasValidationResult.Valid(keyName, WebUtility.UrlDecode(srEncoded), expiry);
    }
}

public sealed record SasValidationResult
{
    public bool IsValid { get; init; }
    public string? FailureReason { get; init; }
    public string? KeyName { get; init; }
    public string? Audience { get; init; }
    public DateTimeOffset? ExpiresAt { get; init; }

    public static SasValidationResult Valid(string keyName, string audience, DateTimeOffset expiresAt) =>
        new() { IsValid = true, KeyName = keyName, Audience = audience, ExpiresAt = expiresAt };

    public static SasValidationResult Invalid(string reason) =>
        new() { IsValid = false, FailureReason = reason };
}
