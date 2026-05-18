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
    public void Validate_CorrectlySignedUnexpiredToken_ReturnsValidResultWithKeyNameAndAudience()
    {
        // Arrange
        var token = BuildToken(Audience, KeyName, Key, DateTimeOffset.UtcNow.AddMinutes(20));
        var keys = new Dictionary<string, string> { [KeyName] = Key };

        // Act
        var result = SasTokenValidator.Validate(token, keys, DateTimeOffset.UtcNow);

        // Assert
        result.IsValid.ShouldBeTrue(result.FailureReason);
        result.KeyName.ShouldBe(KeyName);
        result.Audience.ShouldBe(Audience);
    }

    [Fact]
    public void Validate_ExpiredToken_ReturnsInvalidWithExpiredReason()
    {
        // Arrange
        var token = BuildToken(Audience, KeyName, Key, DateTimeOffset.UtcNow.AddMinutes(-1));
        var keys = new Dictionary<string, string> { [KeyName] = Key };

        // Act
        var result = SasTokenValidator.Validate(token, keys, DateTimeOffset.UtcNow);

        // Assert
        result.IsValid.ShouldBeFalse();
        result.FailureReason!.ShouldContain("expired");
    }

    [Fact]
    public void Validate_TokenSignedWithWrongKey_ReturnsSignatureMismatch()
    {
        // Arrange
        var token = BuildToken(Audience, KeyName, "WRONG-KEY", DateTimeOffset.UtcNow.AddMinutes(20));
        var keys = new Dictionary<string, string> { [KeyName] = Key };

        // Act
        var result = SasTokenValidator.Validate(token, keys, DateTimeOffset.UtcNow);

        // Assert
        result.IsValid.ShouldBeFalse();
        result.FailureReason.ShouldBe("Signature mismatch");
    }

    [Fact]
    public void Validate_UnknownKeyName_ReturnsInvalidWithUnknownKeyNameReason()
    {
        // Arrange
        var token = BuildToken(Audience, "SomeOtherPolicy", Key, DateTimeOffset.UtcNow.AddMinutes(20));
        var keys = new Dictionary<string, string> { [KeyName] = Key };

        // Act
        var result = SasTokenValidator.Validate(token, keys, DateTimeOffset.UtcNow);

        // Assert
        result.IsValid.ShouldBeFalse();
        result.FailureReason!.ShouldContain("Unknown key name");
    }

    [Fact]
    public void Validate_MalformedToken_ReturnsInvalid()
    {
        // Arrange + Act
        var result = SasTokenValidator.Validate("not-a-sas-token", new Dictionary<string, string>(), DateTimeOffset.UtcNow);

        // Assert
        result.IsValid.ShouldBeFalse();
    }

    [Fact]
    public void Validate_NullOrEmptyToken_ReturnsInvalid()
    {
        // Arrange
        var keys = new Dictionary<string, string>();
        var now = DateTimeOffset.UtcNow;

        // Act
        var nullResult = SasTokenValidator.Validate(null, keys, now);
        var emptyResult = SasTokenValidator.Validate("", keys, now);

        // Assert
        nullResult.IsValid.ShouldBeFalse();
        emptyResult.IsValid.ShouldBeFalse();
    }
}
