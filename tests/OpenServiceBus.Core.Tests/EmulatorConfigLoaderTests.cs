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
  public void LoadFromJson_AllAdvancedFieldsSet_AllProjectedOntoDescriptor()
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

    // Assert -  fields are all now honored from config.json.
    result.Queues.Count.ShouldBe(1);
    result.Queues[0].RequiresSession.ShouldBeTrue();
    result.Queues[0].RequiresDuplicateDetection.ShouldBeTrue();
    result.Queues[0].ForwardTo.ShouldBe("other");
    result.Queues[0].ForwardDeadLetteredMessagesTo.ShouldBe("other-dlq");
    result.Warnings.Count.ShouldBe(0);
  }

  [Fact]
  public void LoadFromJson_TopicsDeclared_AreProjectedOntoTopicDescriptors()
  {
    // Arrange - topics + nested subscriptions + a SQL rule.
    const string json = """
                            {
                              "UserConfig": {
                                "Namespaces": [
                                  {
                                    "Name": "ns",
                                    "Queues": [],
                                    "Topics": [
                                      {
                                        "Name": "events",
                                        "Properties": { "DefaultMessageTimeToLive": "PT1H" },
                                        "Subscriptions": [
                                          {
                                            "Name": "all",
                                            "Properties": { "MaxDeliveryCount": 7 }
                                          },
                                          {
                                            "Name": "eu-only",
                                            "Properties": { "RequiresSession": true, "ForwardTo": "downstream" },
                                            "Rules": [
                                              {
                                                "Name": "EuFilter",
                                                "Properties": {
                                                  "FilterType": "Sql",
                                                  "SqlFilter": { "SqlExpression": "region = 'eu'" }
                                                }
                                              }
                                            ]
                                          }
                                        ]
                                      }
                                    ]
                                  }
                                ]
                              }
                            }
                            """;

    // Act
    var result = EmulatorConfigLoader.LoadFromJson(json);

    // Assert - topic, subscriptions, and rules all surface; no warnings.
    result.Topics.Count.ShouldBe(1);
    result.Topics[0].Name.ShouldBe("events");
    result.Topics[0].DefaultMessageTimeToLive.ShouldBe(TimeSpan.FromHours(1));

    result.Subscriptions.Count.ShouldBe(2);
    var euOnly = result.Subscriptions.Single(s => s.Name == "eu-only");
    euOnly.TopicName.ShouldBe("events");
    euOnly.RequiresSession.ShouldBeTrue();
    euOnly.ForwardTo.ShouldBe("downstream");

    result.Rules.Count.ShouldBe(1);
    result.Rules[0].SubscriptionName.ShouldBe("eu-only");
    result.Rules[0].Filter.ShouldBeOfType<OpenServiceBus.Core.Filters.SqlFilter>();
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
