namespace OpenServiceBus.Core.Tests;

public class SmokeTests
{
    [Fact]
    public void Solution_compiles_and_test_runner_executes()
    {
        // M0 sanity check: this exists purely to confirm the test pipeline runs end-to-end.
        // Real coverage starts in M1 (connection lifecycle), M1.5 (CBS), M2 (send), etc.
        (1 + 1).ShouldBe(2);
    }
}
