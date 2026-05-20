namespace OpenServiceBus.Core.Tests;

public class SmokeTests
{
    [Fact]
    public void Addition_OnePlusOne_ReturnsTwo()
    {
        // Sanity check: this exists purely to confirm the test pipeline runs end-to-end.
        // Real coverage starts in the connection-lifecycle, CBS, and send tests.

        // Arrange
        const int left = 1;
        const int right = 1;

        // Act
        var sum = left + right;

        // Assert
        sum.ShouldBe(2);
    }
}
