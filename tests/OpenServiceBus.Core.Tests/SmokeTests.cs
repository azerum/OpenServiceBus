namespace OpenServiceBus.Core.Tests;

public class SmokeTests
{
    [Fact]
    public void Addition_OnePlusOne_ReturnsTwo()
    {
        // M0 sanity check: this exists purely to confirm the test pipeline runs end-to-end.
        // Real coverage starts in M1 (connection lifecycle), M1.5 (CBS), M2 (send), etc.

        // Arrange
        const int left = 1;
        const int right = 1;

        // Act
        var sum = left + right;

        // Assert
        sum.ShouldBe(2);
    }
}
