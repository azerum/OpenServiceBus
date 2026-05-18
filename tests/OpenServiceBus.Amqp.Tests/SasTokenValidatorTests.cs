using System.Net;
using System.Security.Cryptography;
using System.Text;
using OpenServiceBus.Amqp.Hosting;

namespace OpenServiceBus.Amqp.Tests;

public class SasTokenValidatorTests
{
    private const string Audience = "sb://127.0.0.1/myqueue";
    private const string KeyName = "RootManageSharedAccessKey";
    private const string Key = "SAS_KEY_VALUE";

    /// <summary>
    /// Recreate exactly what Azure.Messaging.ServiceBus's <c>SharedAccessSignature.BuildSignature</c> does
    /// so the validator is verified against a real-world token shape.
    /// </summary>
    private static string BuildToken(string audience, string keyName, string key, DateTimeOffset expiresAt)
    {
        var encodedAudience = WebUtility.UrlEncode(audience);
        var expiration = expiresAt.ToUnixTimeSeconds().ToString();
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(key));
        var signature = Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes($"{encodedAudience}\n{expiration}")));
        return $"SharedAccessSignature sr={encodedAudience}&sig={WebUtility.UrlEncode(signature)}&se={WebUtility.UrlEncode(expiration)}&skn={WebUtility.UrlEncode(keyName)}";
    }

    [Fact]
    public void Validate_accepts_a_correctly_signed_unexpired_token()
    {
        var token = BuildToken(Audience, KeyName, Key, DateTimeOffset.UtcNow.AddMinutes(20));
        var keys = new Dictionary<string, string> { [KeyName] = Key };
        var result = SasTokenValidator.Validate(token, keys, DateTimeOffset.UtcNow);
        result.IsValid.ShouldBeTrue(result.FailureReason);
        result.KeyName.ShouldBe(KeyName);
        result.Audience.ShouldBe(Audience);
    }

    [Fact]
    public void Validate_rejects_an_expired_token()
    {
        var token = BuildToken(Audience, KeyName, Key, DateTimeOffset.UtcNow.AddMinutes(-1));
        var keys = new Dictionary<string, string> { [KeyName] = Key };
        var result = SasTokenValidator.Validate(token, keys, DateTimeOffset.UtcNow);
        result.IsValid.ShouldBeFalse();
        result.FailureReason!.ShouldContain("expired");
    }

    [Fact]
    public void Validate_rejects_a_token_signed_with_the_wrong_key()
    {
        var token = BuildToken(Audience, KeyName, "WRONG-KEY", DateTimeOffset.UtcNow.AddMinutes(20));
        var keys = new Dictionary<string, string> { [KeyName] = Key };
        var result = SasTokenValidator.Validate(token, keys, DateTimeOffset.UtcNow);
        result.IsValid.ShouldBeFalse();
        result.FailureReason.ShouldBe("Signature mismatch");
    }

    [Fact]
    public void Validate_rejects_when_key_name_is_unknown()
    {
        var token = BuildToken(Audience, "SomeOtherPolicy", Key, DateTimeOffset.UtcNow.AddMinutes(20));
        var keys = new Dictionary<string, string> { [KeyName] = Key };
        var result = SasTokenValidator.Validate(token, keys, DateTimeOffset.UtcNow);
        result.IsValid.ShouldBeFalse();
        result.FailureReason!.ShouldContain("Unknown key name");
    }

    [Fact]
    public void Validate_rejects_a_malformed_token()
    {
        var result = SasTokenValidator.Validate("not-a-sas-token", new Dictionary<string, string>(), DateTimeOffset.UtcNow);
        result.IsValid.ShouldBeFalse();
    }

    [Fact]
    public void Validate_rejects_a_null_or_empty_token()
    {
        SasTokenValidator.Validate(null, new Dictionary<string, string>(), DateTimeOffset.UtcNow).IsValid.ShouldBeFalse();
        SasTokenValidator.Validate("", new Dictionary<string, string>(), DateTimeOffset.UtcNow).IsValid.ShouldBeFalse();
    }
}
