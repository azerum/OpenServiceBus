using OpenServiceBus.Core.Configuration;

namespace OpenServiceBus.Core.Tests;

public class EmulatorConfigLoaderTests
{
    [Fact]
    public void LoadFromJson_OneQueueWithAllSupportedFields_ProjectsThemOntoQueueDescriptor()
    {
        // Arrange
        const string json = """
                            {
                              "UserConfig": {
                                "Namespaces": [
                                  {
                                    "Name": "ns",
                                    "Queues": [
                                      {
                                        "Name": "orders",
                                        "Properties": {
                                          "LockDuration": "PT30S",
                                          "MaxDeliveryCount": 7,
                                          "DefaultMessageTimeToLive": "PT5M",
                                          "DeadLetteringOnMessageExpiration": true
                                        }
                                      }
                                    ]
                                  }
                                ]
                              }
                            }
                            """;

        // Act
        var result = EmulatorConfigLoader.LoadFromJson(json);

        // Assert
        result.Queues.Count.ShouldBe(1);
        var q = result.Queues[0];
        q.Name.ShouldBe("orders");
        q.LockDuration.ShouldBe(TimeSpan.FromSeconds(30));
        q.MaxDeliveryCount.ShouldBe(7);
        q.DefaultMessageTimeToLive.ShouldBe(TimeSpan.FromMinutes(5));
        q.DeadLetteringOnMessageExpiration.ShouldBeTrue();
        result.Warnings.ShouldBeEmpty();
    }

    [Fact]
    public void LoadFromJson_MissingProperties_UsesQueueDescriptorDefaults()
    {
        // Arrange
        const string json = """
                            {
                              "UserConfig": {
                                "Namespaces": [
                                  { "Name": "ns", "Queues": [ { "Name": "bare" } ] }
                                ]
                              }
                            }
                            """;

        // Act
        var result = EmulatorConfigLoader.LoadFromJson(json);

        // Assert
        result.Queues.Count.ShouldBe(1);
        var q = result.Queues[0];
        q.Name.ShouldBe("bare");
        q.LockDuration.ShouldBe(TimeSpan.FromSeconds(60));
        q.MaxDeliveryCount.ShouldBe(10);
        q.DefaultMessageTimeToLive.ShouldBeNull();
        q.DeadLetteringOnMessageExpiration.ShouldBeFalse();
    }

    [Fact]
    public void LoadFromJson_UnsupportedFieldsSet_EmitsWarningsButStillCreatesQueue()
    {
        // Arrange
        const string json = """
                            {
                              "UserConfig": {
                                "Namespaces": [
                                  {
                                    "Name": "ns",
                                    "Queues": [
                                      {
                                        "Name": "compat",
                                        "Properties": {
                                          "RequiresSession": true,
                                          "RequiresDuplicateDetection": true,
                                          "ForwardTo": "other",
                                          "ForwardDeadLetteredMessagesTo": "other-dlq"
                                        }
                                      }
                                    ]
                                  }
                                ]
                              }
                            }
                            """;

        // Act
        var result = EmulatorConfigLoader.LoadFromJson(json);

        // Assert
        // RequiresSession (M14) and RequiresDuplicateDetection (M15) are now honored, no warning.
        // Forwarding still warns until M16 lands.
        result.Queues.Count.ShouldBe(1);
        result.Queues[0].RequiresSession.ShouldBeTrue();
        result.Queues[0].RequiresDuplicateDetection.ShouldBeTrue();
        result.Warnings.ShouldContain(w => w.Contains("ForwardTo"));
        result.Warnings.ShouldContain(w => w.Contains("ForwardDeadLetteredMessagesTo"));
        result.Warnings.Count.ShouldBe(2);
    }

    [Fact]
    public void LoadFromJson_TopicsDeclared_EmitsWarningAndIgnoresThem()
    {
        // Arrange
        const string json = """
                            {
                              "UserConfig": {
                                "Namespaces": [
                                  {
                                    "Name": "ns",
                                    "Queues": [],
                                    "Topics": [ { "Name": "t1" }, { "Name": "t2" } ]
                                  }
                                ]
                              }
                            }
                            """;

        // Act
        var result = EmulatorConfigLoader.LoadFromJson(json);

        // Assert
        result.Queues.ShouldBeEmpty();
        result.Warnings.ShouldContain(w => w.Contains("topic"));
    }

    [Fact]
    public void LoadFromJson_InvalidIsoDuration_EmitsWarningAndFallsBackToDefault()
    {
        // Arrange
        const string json = """
                            {
                              "UserConfig": {
                                "Namespaces": [
                                  {
                                    "Name": "ns",
                                    "Queues": [
                                      { "Name": "bad", "Properties": { "LockDuration": "not-a-duration" } }
                                    ]
                                  }
                                ]
                              }
                            }
                            """;

        // Act
        var result = EmulatorConfigLoader.LoadFromJson(json);

        // Assert
        result.Queues.Count.ShouldBe(1);
        result.Queues[0].LockDuration.ShouldBe(TimeSpan.FromSeconds(60), "falls back to QueueDescriptor default");
        result.Warnings.ShouldContain(w => w.Contains("LockDuration") && w.Contains("ISO 8601"));
    }

    [Fact]
    public void LoadFromJson_EmptyQueueName_SkipsQueueAndWarns()
    {
        // Arrange
        const string json = """
                            {
                              "UserConfig": {
                                "Namespaces": [
                                  { "Name": "ns", "Queues": [ { "Name": "", "Properties": {} }, { "Name": "ok" } ] }
                                ]
                              }
                            }
                            """;

        // Act
        var result = EmulatorConfigLoader.LoadFromJson(json);

        // Assert
        result.Queues.Count.ShouldBe(1);
        result.Queues[0].Name.ShouldBe("ok");
        result.Warnings.ShouldContain(w => w.Contains("empty Name"));
    }
}
