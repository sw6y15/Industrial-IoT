// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Services {
    using Microsoft.Azure.IIoT.OpcUa.Publisher;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Storage.Default;
    using Microsoft.Azure.IIoT.OpcUa.Registry;
    using Microsoft.Azure.IIoT.OpcUa.Registry.Models;
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;
    using Microsoft.Azure.IIoT.Serializers;
    using Microsoft.Azure.IIoT.Serializers.NewtonSoft;
    using Microsoft.Azure.IIoT.Exceptions;
    using Microsoft.Azure.IIoT.Storage;
    using Microsoft.Azure.IIoT.Storage.Default;
    using Autofac.Extras.Moq;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Threading;
    using Xunit;
    using Xunit.Sdk;
    using Autofac;
    using Moq;

    /// <summary>
    /// Writer group registry tests
    /// </summary>
    public class WriterGroupRegistryTests {

        [Fact]
        public async Task AddWriterButNoGroupExistsShouldExceptAsync() {

            using (var mock = Setup((v, q) => {
                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();

                await Assert.ThrowsAsync<ArgumentNullException>(async () => {
                    await service.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                        EndpointId = null,
                        DataSetName = "Test",
                        WriterGroupId = "doesnotexist"
                    });
                });

                await Assert.ThrowsAsync<ArgumentException>(async () => {
                    await service.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                        EndpointId = "someendpointthatexists",
                        DataSetName = "Test",
                        WriterGroupId = "doesnotexist"
                    });
                });

            }
        }

        [Fact]
        public async Task AddWriterButGroupNotInSiteShouldExceptAsync() {

            using (var mock = Setup((v, q) => {
                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();

                // Act
                var result = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "Test",
                    SiteId = "doesnotexist"
                });

                await Assert.ThrowsAsync<ArgumentException>(async () => {
                    await service.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                        EndpointId = "someendpointthatexists",
                        DataSetName = "Test",
                        WriterGroupId = result.WriterGroupId
                    });
                });

            }
        }

        [Fact]
        public async Task GetOrRemoveItemsNotFoundShouldThrowAsync() {

            using (var mock = Setup((v, q) => {
                var id = q
                    .Replace("SELECT * FROM r WHERE r.WriterGroupId = '", "")
                    .Replace("' AND r.ClassType = 'DataSetWriter'", "");
                if (Guid.TryParse(id, out var writerGroupId)) {
                    // Get writers not disabled
                    return v
                        .Where(o => o.Value["WriterGroupId"] == writerGroupId.ToString())
                        .Where(o => o.Value["ClassType"] == "DataSetWriter");
                }
                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();

                // Assert
                var id = Guid.NewGuid().ToString();
                await Assert.ThrowsAsync<ResourceNotFoundException>(() => service.GetDataSetWriterAsync(id));
                await Assert.ThrowsAsync<ResourceNotFoundException>(() => service.RemoveDataSetWriterAsync(id, "test"));
                await Assert.ThrowsAsync<ResourceNotFoundException>(() => groups.GetWriterGroupAsync(id));
                await Assert.ThrowsAsync<ResourceNotFoundException>(() => groups.RemoveWriterGroupAsync(id, "test"));
                await Assert.ThrowsAsync<ResourceNotFoundException>(() => service.GetEventDataSetAsync(id));
                await Assert.ThrowsAsync<ResourceNotFoundException>(() => service.RemoveEventDataSetAsync(id, "test"));
                await Assert.ThrowsAsync<ResourceNotFoundException>(() => service.RemoveDataSetVariableAsync(id, id, "test"));
            }
        }

        [Fact]
        public async Task AddWriterWithExistingGroupTestAsync() {

            using (var mock = Setup((v, q) => {
                var id = q
                    .Replace("SELECT * FROM r WHERE r.DataSetWriterId = '", "")
                    .Replace("' AND r.ClassType = 'DataSetEntity' AND r.Type = 'Variable'", "");
                if (Guid.TryParse(id, out var dataSetWriterid)) {
                    // Get variables
                    return v
                        .Where(o => o.Value["DataSetWriterId"] == dataSetWriterid.ToString())
                        .Where(o => o.Value["ClassType"] == "DataSetEntity")
                        .Where(o => o.Value["Type"] == "Variable");
                }

                id = q
                    .Replace("SELECT * FROM r WHERE r.WriterGroupId = '", "")
                    .Replace("' AND r.IsDisabled = false AND r.ClassType = 'DataSetWriter'", "");
                if (Guid.TryParse(id, out var writerGroupId)) {
                    // Get writers
                    return v
                        .Where(o => o.Value["WriterGroupId"] == writerGroupId.ToString())
                        .Where(o => o.Value["ClassType"] == "DataSetWriter")
                        .Where(o => o.Value["IsDisabled"] == false);
                }

                id = q
                    .Replace("SELECT * FROM r WHERE r.WriterGroupId = '", "")
                    .Replace("' AND r.ClassType = 'DataSetWriter'", "");
                if (Guid.TryParse(id, out writerGroupId)) {
                    // Get writers not disabled
                    return v
                        .Where(o => o.Value["WriterGroupId"] == writerGroupId.ToString())
                        .Where(o => o.Value["ClassType"] == "DataSetWriter");
                }

                id = q
                    .Replace("SELECT * FROM r WHERE r.DataSetWriterId = '", "")
                    .Replace("' AND r.ClassType = 'DataSetEntity'", "");
                if (Guid.TryParse(id, out dataSetWriterid)) {
                    // Get variables and entities
                    return v
                        .Where(o => o.Value["DataSetWriterId"] == dataSetWriterid.ToString())
                        .Where(o => o.Value["ClassType"] == "DataSetEntity");
                }

                var expected = "SELECT * FROM r WHERE r.ClassType = 'DataSetWriter'";
                if (q == expected) {
                    // Get variables and entities
                    return v
                        .Where(o => o.Value["ClassType"] == "DataSetWriter");
                }

                expected = "SELECT * FROM r WHERE r.ClassType = 'WriterGroup'";
                if (q == expected) {
                    // Get variables and entities
                    return v
                        .Where(o => o.Value["ClassType"] == "WriterGroup");
                }

                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();

                // Act
                var result1 = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "Test",
                    SiteId = "fakesite" // See below
                });

                var result2  = await service.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                    EndpointId = "endpoint1",
                    DataSetName = "Test",
                    KeyFrameInterval = TimeSpan.FromSeconds(1),
                    SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                        Priority = 1
                    },
                    WriterGroupId = result1.WriterGroupId
                });

                var writer = await service.GetDataSetWriterAsync(result2.DataSetWriterId);

                // Assert
                Assert.NotNull(writer);
                Assert.Equal(result2.DataSetWriterId, writer.DataSetWriterId);
                Assert.NotNull(writer.DataSet);
                Assert.NotNull(writer.DataSet.DataSetSource);
                Assert.NotNull(writer.DataSet.DataSetSource.Connection);
                Assert.NotNull(writer.DataSet.DataSetSource.Connection.Endpoint);
                Assert.Equal("fakeurl", writer.DataSet.DataSetSource.Connection.Endpoint.Url);
                Assert.Equal(TimeSpan.FromSeconds(1), writer.KeyFrameInterval);
                Assert.NotNull(writer.DataSet.DataSetSource.SubscriptionSettings);
                Assert.Equal((byte)1, writer.DataSet.DataSetSource.SubscriptionSettings.Priority);

                // Act
                var writerresult = await service.ListDataSetWritersAsync();

                // Assert
                Assert.NotNull(writerresult.DataSetWriters);
                Assert.Null(writerresult.ContinuationToken);
                Assert.Single(writerresult.DataSetWriters);
                Assert.Collection(writerresult.DataSetWriters, writer2 => {
                    Assert.Equal(writer.DataSetWriterId, writer2.DataSetWriterId);
                    Assert.Equal("endpoint1", writer2.DataSet.EndpointId);
                    Assert.Equal((byte)1, writer2.DataSet.SubscriptionSettings.Priority);
                });

                // Act
                var group = await groups.GetWriterGroupAsync(result1.WriterGroupId);

                // Assert
                Assert.NotNull(group);
                Assert.Equal(result1.WriterGroupId, group.WriterGroupId);
                Assert.Equal("Test", group.Name);
                Assert.Equal("fakesite", group.SiteId);
                Assert.Single(group.DataSetWriters);
                Assert.Collection(group.DataSetWriters, writer2 => {
                    Assert.Equal(writer.DataSetWriterId, writer2.DataSetWriterId);
                    Assert.Equal("fakeurl", writer2.DataSet.DataSetSource.Connection.Endpoint.Url);
                    Assert.Equal((byte)1, writer2.DataSet.DataSetSource.SubscriptionSettings.Priority);
                });

                // Act
                var groupresult = await groups.ListWriterGroupsAsync();

                // Assert
                Assert.NotNull(groupresult.WriterGroups);
                Assert.Null(groupresult.ContinuationToken);
                Assert.Single(groupresult.WriterGroups);
                Assert.Collection(groupresult.WriterGroups, group2 => {
                    Assert.Equal(group.WriterGroupId, group2.WriterGroupId);
                    Assert.Equal(group.SiteId, group2.SiteId);
                    Assert.Equal(group.Name, group2.Name);
                });

                // Act/Assert
                await Assert.ThrowsAsync<ResourceInvalidStateException>(() => groups.RemoveWriterGroupAsync(
                    group.WriterGroupId, group.GenerationId));
                await Assert.ThrowsAsync<ResourceOutOfDateException>(() => service.RemoveDataSetWriterAsync(
                    writer.DataSetWriterId, "invalidetag"));

                // Act
                await service.RemoveDataSetWriterAsync(writer.DataSetWriterId, writer.GenerationId);

                // Assert
                await Assert.ThrowsAsync<ResourceNotFoundException>(() => service.GetDataSetWriterAsync(
                    writer.DataSetWriterId));
                await Assert.ThrowsAsync<ResourceOutOfDateException>(() => groups.RemoveWriterGroupAsync(
                    group.WriterGroupId, "invalidetag"));

                // Act
                await groups.RemoveWriterGroupAsync(group.WriterGroupId, group.GenerationId);

                // Assert
                await Assert.ThrowsAsync<ResourceNotFoundException>(() => groups.GetWriterGroupAsync(
                    group.WriterGroupId));
            }
        }

        [Fact]
        public async Task AddWriterToDefaultGroupTestAsync() {

            var writerGroupId = "fakesite";
            using (var mock = Setup((v, q) => {
                var id = q
                    .Replace("SELECT * FROM r WHERE r.DataSetWriterId = '", "")
                    .Replace("' AND r.ClassType = 'DataSetEntity' AND r.Type = 'Variable'", "");
                if (Guid.TryParse(id, out var dataSetWriterid)) {
                    // Get variables
                    return v
                        .Where(o => o.Value["DataSetWriterId"] == dataSetWriterid.ToString())
                        .Where(o => o.Value["ClassType"] == "DataSetEntity")
                        .Where(o => o.Value["Type"] == "Variable");
                }

                var expected = $"SELECT * FROM r WHERE r.WriterGroupId = '{writerGroupId}' AND " +
                    "r.IsDisabled = false AND r.ClassType = 'DataSetWriter'";
                if (expected == q) {
                    // Get writers
                    return v
                        .Where(o => o.Value["WriterGroupId"] == writerGroupId)
                        .Where(o => o.Value["ClassType"] == "DataSetWriter")
                        .Where(o => o.Value["IsDisabled"] == false);
                }

                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();

                // Act
                var result2 = await service.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                    EndpointId = "endpoint1",
                    DataSetName = "Test",
                    KeyFrameInterval = TimeSpan.FromSeconds(1),
                    SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                        Priority = 1
                    }
                });

                var writer = await service.GetDataSetWriterAsync(result2.DataSetWriterId);

                // Assert
                Assert.NotNull(writer);
                Assert.Equal(result2.DataSetWriterId, writer.DataSetWriterId);
                Assert.NotNull(writer.DataSet);
                Assert.NotNull(writer.DataSet.DataSetSource);
                Assert.NotNull(writer.DataSet.DataSetSource.Connection);
                Assert.NotNull(writer.DataSet.DataSetSource.Connection.Endpoint);
                Assert.Equal("fakeurl", writer.DataSet.DataSetSource.Connection.Endpoint.Url);
                Assert.Equal(TimeSpan.FromSeconds(1), writer.KeyFrameInterval);
                Assert.NotNull(writer.DataSet.DataSetSource.SubscriptionSettings);
                Assert.Equal((byte)1, writer.DataSet.DataSetSource.SubscriptionSettings.Priority);

                // Act
                var group = await groups.GetWriterGroupAsync(writerGroupId);

                // Assert
                Assert.NotNull(group);
                Assert.Equal(writerGroupId, group.WriterGroupId);
                Assert.Equal("fakesite", group.SiteId);
                Assert.Single(group.DataSetWriters);
                Assert.Collection(group.DataSetWriters, writer2 => {
                    Assert.Equal(writer.DataSetWriterId, writer2.DataSetWriterId);
                    Assert.Equal("fakeurl", writer2.DataSet.DataSetSource.Connection.Endpoint.Url);
                    Assert.Equal((byte)1, writer2.DataSet.DataSetSource.SubscriptionSettings.Priority);
                });
            }
        }

        [Fact]
        public async Task AddVariablesToWriterTest1Async() {

            using (var mock = Setup((v, q) => {
                var id = q
                    .Replace("SELECT * FROM r WHERE r.DataSetWriterId = '", "")
                    .Replace("' AND r.ClassType = 'DataSetEntity' AND r.Type = 'Variable'", "");
                if (Guid.TryParse(id, out var dataSetWriterid)) {
                    // Get variables
                    return v
                        .Where(o => o.Value["DataSetWriterId"] == dataSetWriterid.ToString())
                        .Where(o => o.Value["ClassType"] == "DataSetEntity")
                        .Where(o => o.Value["Type"] == "Variable");
                }
                id = q
                    .Replace("SELECT * FROM r WHERE r.DataSetWriterId = '", "")
                    .Replace("' AND r.ClassType = 'DataSetEntity'", "");
                if (Guid.TryParse(id, out dataSetWriterid)) {
                    // Get variables and entities
                    return v
                        .Where(o => o.Value["DataSetWriterId"] == dataSetWriterid.ToString())
                        .Where(o => o.Value["ClassType"] == "DataSetEntity");
                }

                id = q
                    .Replace("SELECT * FROM r WHERE r.WriterGroupId = '", "")
                    .Replace("' AND r.ClassType = 'DataSetWriter'", "");
                if (Guid.TryParse(id, out var writerGroupId)) {
                    // Get writers not disabled
                    return v
                        .Where(o => o.Value["WriterGroupId"] == writerGroupId.ToString())
                        .Where(o => o.Value["ClassType"] == "DataSetWriter");
                }
                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();

                // Act
                var group = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "Test",
                    SiteId = "fakesite" // See below
                });

                var writer = await service.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                    EndpointId = "endpoint1",
                    DataSetName = "Test",
                    KeyFrameInterval = TimeSpan.FromSeconds(1),
                    SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                        Priority = 1
                    },
                    WriterGroupId = group.WriterGroupId
                });

                var variables = new List<DataSetAddVariableResultModel>();
                for (var i = 0; i < 10; i++) {
                    var variable = await service.AddDataSetVariableAsync(writer.DataSetWriterId,
                        new DataSetAddVariableRequestModel {
                            PublishedVariableNodeId = "i=2554",
                            HeartbeatInterval = TimeSpan.FromDays(1)
                        });
                    variables.Add(variable);
                }

                var found = await service.ListAllDataSetVariablesAsync(writer.DataSetWriterId);

                // Assert
                Assert.Equal(10, found.Count);
                Assert.All(found, v => {
                    Assert.Equal("i=2554", v.PublishedVariableNodeId);
                    Assert.Null(v.PublishedVariableDisplayName);
                    Assert.Equal(TimeSpan.FromDays(1), v.HeartbeatInterval);
                    Assert.Contains(variables, f => f.Id == v.Id);
                });

                var expected = variables.Count;
                foreach (var item in variables) {
                    await Assert.ThrowsAsync<ResourceOutOfDateException>(() => service.RemoveDataSetVariableAsync(
                        writer.DataSetWriterId, item.Id, "invalidetag"));

                    // Act
                    await service.RemoveDataSetVariableAsync(writer.DataSetWriterId, item.Id, item.GenerationId);

                    found = await service.ListAllDataSetVariablesAsync(writer.DataSetWriterId);
                    Assert.Equal(--expected, found.Count);
                }

                // Act
                await service.RemoveDataSetWriterAsync(writer.DataSetWriterId, writer.GenerationId);
                await groups.RemoveWriterGroupAsync(group.WriterGroupId, group.GenerationId);

                // Assert
                await Assert.ThrowsAsync<ResourceNotFoundException>(() => groups.GetWriterGroupAsync(
                    group.WriterGroupId));
            }
        }

        [Fact]
        public async Task AddVariablesToWriterTest2Async() {

            using (var mock = Setup((v, q) => {
                var id = q
                    .Replace("SELECT * FROM r WHERE r.DataSetWriterId = '", "")
                    .Replace("' AND r.ClassType = 'DataSetEntity' AND r.Type = 'Variable'", "");
                if (Guid.TryParse(id, out var dataSetWriterid)) {
                    // Get variables
                    return v
                        .Where(o => o.Value["DataSetWriterId"] == dataSetWriterid.ToString())
                        .Where(o => o.Value["ClassType"] == "DataSetEntity")
                        .Where(o => o.Value["Type"] == "Variable");
                }
                id = q
                    .Replace("SELECT * FROM r WHERE r.DataSetWriterId = '", "")
                    .Replace("' AND r.ClassType = 'DataSetEntity'", "");
                if (Guid.TryParse(id, out dataSetWriterid)) {
                    // Get variables and entities
                    return v
                        .Where(o => o.Value["DataSetWriterId"] == dataSetWriterid.ToString())
                        .Where(o => o.Value["ClassType"] == "DataSetEntity");
                }

                id = q
                    .Replace("SELECT * FROM r WHERE r.WriterGroupId = '", "")
                    .Replace("' AND r.ClassType = 'DataSetWriter'", "");
                if (Guid.TryParse(id, out var writerGroupId)) {
                    // Get writers not disabled
                    return v
                        .Where(o => o.Value["WriterGroupId"] == writerGroupId.ToString())
                        .Where(o => o.Value["ClassType"] == "DataSetWriter");
                }
                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();

                // Act
                var group = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "Test",
                    SiteId = "fakesite" // See below
                });

                var writer = await service.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                    EndpointId = "endpoint1",
                    DataSetName = "Test",
                    KeyFrameInterval = TimeSpan.FromSeconds(1),
                    SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                        Priority = 1
                    },
                    WriterGroupId = group.WriterGroupId
                });

                var variables = new List<DataSetAddVariableResultModel>();
                for (var i = 0; i < 10; i++) {
                    var variable = await service.AddDataSetVariableAsync(writer.DataSetWriterId,
                        new DataSetAddVariableRequestModel {
                            PublishedVariableNodeId = "i=2554",
                            HeartbeatInterval = TimeSpan.FromDays(1)
                        });
                    variables.Add(variable);
                }

                var found = await service.ListAllDataSetVariablesAsync(writer.DataSetWriterId);

                // Assert
                Assert.Equal(10, found.Count);
                Assert.All(found, v => {
                    Assert.Equal("i=2554", v.PublishedVariableNodeId);
                    Assert.Null(v.PublishedVariableDisplayName);
                    Assert.Equal(TimeSpan.FromDays(1), v.HeartbeatInterval);
                    Assert.Contains(variables, f => f.Id == v.Id);
                });

                // Act
                await service.RemoveDataSetWriterAsync(writer.DataSetWriterId, writer.GenerationId);

                // Assert
                found = await service.ListAllDataSetVariablesAsync(writer.DataSetWriterId);
                Assert.NotNull(found);
                Assert.Empty(found);

                await groups.RemoveWriterGroupAsync(group.WriterGroupId, group.GenerationId);

                // Assert
                await Assert.ThrowsAsync<ResourceNotFoundException>(() => groups.GetWriterGroupAsync(
                    group.WriterGroupId));
            }
        }

        [Fact]
        public async Task AddVariablesToDataSetWriterTestAsync() {

            using (var mock = Setup((v, q) => {
                var id = q
                    .Replace("SELECT * FROM r WHERE r.DataSetWriterId = '", "")
                    .Replace("' AND r.ClassType = 'DataSetEntity' AND r.Type = 'Variable'", "");
                if (Guid.TryParse(id, out var dataSetWriterid)) {
                    // Get variables
                    return v
                        .Where(o => o.Value["DataSetWriterId"] == dataSetWriterid.ToString())
                        .Where(o => o.Value["ClassType"] == "DataSetEntity")
                        .Where(o => o.Value["Type"] == "Variable");
                }
                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IDataSetBatchOperations batch = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();

                // Act
                var group = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "Test",
                    SiteId = "fakesite" // See below
                });

                var writer = await service.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                    EndpointId = "endpoint1",
                    DataSetName = "Test",
                    KeyFrameInterval = TimeSpan.FromSeconds(1),
                    SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                        Priority = 1
                    },
                    WriterGroupId = group.WriterGroupId
                });

                var result = await batch.AddVariablesToDataSetWriterAsync(writer.DataSetWriterId,
                    new DataSetAddVariableBatchRequestModel {
                        DataSetPublishingInterval = TimeSpan.FromSeconds(1),
                        Variables = new List<DataSetAddVariableRequestModel> {
                            new DataSetAddVariableRequestModel {
                                PublishedVariableNodeId = "i=2554",
                                HeartbeatInterval = TimeSpan.FromDays(1)
                            },
                            new DataSetAddVariableRequestModel {
                                PublishedVariableNodeId = "i=2555",
                                HeartbeatInterval = TimeSpan.FromDays(1)
                            },
                            new DataSetAddVariableRequestModel {
                                PublishedVariableNodeId = "i=2556",
                                HeartbeatInterval = TimeSpan.FromDays(1)
                            }
                        }
                    });

                var found = await service.ListAllDataSetVariablesAsync(writer.DataSetWriterId);

                // Assert
                Assert.Equal(3, found.Count);
                Assert.All(found, v => {
                    Assert.Equal(TimeSpan.FromDays(1), v.HeartbeatInterval);
                    Assert.Contains(result.Results, f => f.Id == v.Id);
                    Assert.Null(v.DeadbandValue);
                    Assert.Null(v.MonitoringMode);
                    Assert.Null(v.DeadbandType);
                    Assert.Null(v.DataChangeFilter);
                    Assert.Null(v.DiscardNew);
                    Assert.Null(v.QueueSize);
                    Assert.Null(v.PublishedVariableDisplayName);
                    Assert.Null(v.SubstituteValue);
                    Assert.Null(v.SamplingInterval);
                    Assert.Null(v.TriggerId);
                });

                await Assert.ThrowsAsync<ResourceOutOfDateException>(() =>
                    service.UpdateDataSetVariableAsync(writer.DataSetWriterId, found.Last().Id,
                    new DataSetUpdateVariableRequestModel {
                        GenerationId = "badgenerationid",
                        MonitoringMode = Models.MonitoringMode.Reporting
                    }));
                await service.UpdateDataSetVariableAsync(writer.DataSetWriterId, found.Last().Id,
                    new DataSetUpdateVariableRequestModel {
                        GenerationId = found.Last().GenerationId,
                        MonitoringMode = Models.MonitoringMode.Reporting,
                        DeadbandType = Models.DeadbandType.Percent,
                        DeadbandValue = 0.5,
                        DataChangeFilter = DataChangeTriggerType.StatusValue,
                        DiscardNew = true,
                        HeartbeatInterval = TimeSpan.FromSeconds(2),
                        QueueSize = 44u,
                        PublishedVariableDisplayName = "Test",
                        SamplingInterval = TimeSpan.FromSeconds(4),
                        SubstituteValue = "string",
                        TriggerId = "555"
                    });

                var remove = batch.RemoveVariablesFromDataSetWriterAsync(writer.DataSetWriterId,
                    new DataSetRemoveVariableBatchRequestModel {
                        Variables = new List<DataSetRemoveVariableRequestModel> {
                            new DataSetRemoveVariableRequestModel {
                                PublishedVariableNodeId = "i=2554"
                            },
                            new DataSetRemoveVariableRequestModel {
                                PublishedVariableNodeId = "i=2555"
                            }
                        }
                    });

                // Assert
                found = await service.ListAllDataSetVariablesAsync(writer.DataSetWriterId);
                Assert.NotNull(found);
                Assert.Single(found);
                Assert.Equal(0.5, found.Single().DeadbandValue);
                Assert.Equal(Models.MonitoringMode.Reporting, found.Single().MonitoringMode);
                Assert.Equal(Models.DeadbandType.Percent, found.Single().DeadbandType);
                Assert.Equal(DataChangeTriggerType.StatusValue, found.Single().DataChangeFilter);
                Assert.Equal(true, found.Single().DiscardNew);
                Assert.Equal(TimeSpan.FromSeconds(2), found.Single().HeartbeatInterval);
                Assert.Equal(44u, found.Single().QueueSize);
                Assert.Equal("Test", found.Single().PublishedVariableDisplayName);
                Assert.Equal(TimeSpan.FromSeconds(4), found.Single().SamplingInterval);
                Assert.Equal("string", found.Single().SubstituteValue);
                Assert.Equal("555", found.Single().TriggerId);

                await service.UpdateDataSetVariableAsync(writer.DataSetWriterId, found.Last().Id,
                    new DataSetUpdateVariableRequestModel {
                        GenerationId = found.Last().GenerationId,
                        MonitoringMode = Models.MonitoringMode.Disabled,
                        DeadbandType = Models.DeadbandType.Absolute,
                        DeadbandValue = 0.0,
                        DataChangeFilter = DataChangeTriggerType.Status,
                        DiscardNew = false,
                        HeartbeatInterval = TimeSpan.FromSeconds(0),
                        QueueSize = 0u,
                        PublishedVariableDisplayName = "",
                        SamplingInterval = TimeSpan.FromSeconds(0),
                        SubstituteValue = VariantValue.Null,
                        TriggerId = ""
                    });

                // Assert
                found = await service.ListAllDataSetVariablesAsync(writer.DataSetWriterId);
                Assert.NotNull(found);
                Assert.Single(found);
                var v = found.Single();
                Assert.Null(v.DeadbandValue);
                Assert.Null(v.MonitoringMode);
                Assert.Null(v.DeadbandType);
                Assert.Null(v.DataChangeFilter);
                Assert.Null(v.DiscardNew);
                Assert.Null(v.HeartbeatInterval);
                Assert.Null(v.QueueSize);
                Assert.Null(v.PublishedVariableDisplayName);
                Assert.Null(v.SubstituteValue);
                Assert.Null(v.SamplingInterval);
                Assert.Null(v.TriggerId);
            }
        }

        [Fact]
        public async Task UpdateWriterAndGroupTestAsync() {

            using (var mock = Setup((v, q) => {
                var id = q
                    .Replace("SELECT * FROM r WHERE r.DataSetWriterId = '", "")
                    .Replace("' AND r.ClassType = 'DataSetEntity' AND r.Type = 'Variable'", "");
                if (Guid.TryParse(id, out var dataSetWriterid)) {
                    // Get variables
                    return v
                        .Where(o => o.Value["DataSetWriterId"] == dataSetWriterid.ToString())
                        .Where(o => o.Value["ClassType"] == "DataSetEntity")
                        .Where(o => o.Value["Type"] == "Variable");
                }

                id = q
                    .Replace("SELECT * FROM r WHERE r.WriterGroupId = '", "")
                    .Replace("' AND r.IsDisabled = false AND r.ClassType = 'DataSetWriter'", "");
                if (Guid.TryParse(id, out var writerGroupId)) {
                    // Get writers
                    return v
                        .Where(o => o.Value["WriterGroupId"] == writerGroupId.ToString())
                        .Where(o => o.Value["ClassType"] == "DataSetWriter")
                        .Where(o => o.Value["IsDisabled"] == false);
                }
                var expected =
                    "SELECT * FROM r WHERE r.LastState = 'Pending' AND r.ClassType = 'WriterGroup'";
                if (q == expected) {
                    // Get variables and entities
                    return v
                        .Where(o => o.Value["LastState"] == "Pending")
                        .Where(o => o.Value["ClassType"] == "WriterGroup");
                }

                expected =
                    "SELECT * FROM r WHERE r.LastState = 'Disabled' AND r.ClassType = 'WriterGroup'";
                if (q == expected) {
                    // Get variables and entities
                    return v
                        .Where(o => o.Value["LastState"] == "Disabled")
                        .Where(o => o.Value["ClassType"] == "WriterGroup");
                }
                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();

                // Act
                var result1 = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "Test",
                    SiteId = "fakesite" // See below
                });

                await Assert.ThrowsAsync<ResourceOutOfDateException>(() =>
                    groups.UpdateWriterGroupAsync(result1.WriterGroupId,
                    new WriterGroupUpdateRequestModel {
                        GenerationId = "badgenerationid",
                        BatchSize = 5
                    }));
                await groups.UpdateWriterGroupAsync(result1.WriterGroupId,
                    new WriterGroupUpdateRequestModel {
                        GenerationId = result1.GenerationId,
                        BatchSize = 44,
                        HeaderLayoutUri = "HeaderLayoutUri",
                        KeepAliveTime = TimeSpan.FromSeconds(56),
                        LocaleIds = new List<string> {
                            "a", "a", "a"
                        },
                        MessageSettings = new WriterGroupMessageSettingsModel {
                            DataSetOrdering = Models.DataSetOrderingType.AscendingWriterIdSingle,
                            GroupVersion = 34,
                            NetworkMessageContentMask =
                                NetworkMessageContentMask.NetworkMessageHeader |
                                NetworkMessageContentMask.DataSetClassId,
                            PublishingOffset = new List<double> {
                                0.5, 0.5
                            },
                            SamplingOffset = 0.5
                        },
                        Encoding = MessageEncoding.Uadp,
                        Schema = MessageSchema.Samples,
                        Name = "New name",
                        Priority = 66,
                        PublishingInterval = TimeSpan.FromMilliseconds(566)
                    });

                var group = await groups.GetWriterGroupAsync(result1.WriterGroupId);
                Assert.Equal(44, group.BatchSize);
                Assert.Equal("HeaderLayoutUri", group.HeaderLayoutUri);
                Assert.Equal(TimeSpan.FromSeconds(56), group.KeepAliveTime);
                Assert.All(group.LocaleIds, b => Assert.Equal("a", b));
                Assert.NotNull(group.MessageSettings);
                Assert.Equal(Models.DataSetOrderingType.AscendingWriterIdSingle, group.MessageSettings.DataSetOrdering);
                Assert.Equal(34u, group.MessageSettings.GroupVersion);
                Assert.Equal(NetworkMessageContentMask.NetworkMessageHeader | NetworkMessageContentMask.DataSetClassId,
                    group.MessageSettings.NetworkMessageContentMask);
                Assert.All(group.MessageSettings.PublishingOffset, b => Assert.Equal(0.5, b));
                Assert.Equal(0.5, group.MessageSettings.SamplingOffset);
                Assert.Equal(MessageEncoding.Uadp, group.Encoding);
                Assert.Equal(MessageSchema.Samples, group.Schema);
                Assert.Equal("New name", group.Name);
                Assert.Equal((byte)66, group.Priority);
                Assert.Equal(TimeSpan.FromMilliseconds(566), group.PublishingInterval);

                var result2 = await service.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                    EndpointId = "endpoint1",
                    DataSetName = "Test",
                    KeyFrameInterval = TimeSpan.FromSeconds(1),
                    SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                        Priority = 1
                    },
                    WriterGroupId = result1.WriterGroupId
                });

                await Assert.ThrowsAsync<ResourceOutOfDateException>(() =>
                    service.UpdateDataSetWriterAsync(result2.DataSetWriterId,
                    new DataSetWriterUpdateRequestModel {
                        GenerationId = "badgenerationid",
                        KeyFrameInterval = TimeSpan.FromSeconds(5)
                    }));
                await service.UpdateDataSetWriterAsync(result2.DataSetWriterId,
                    new DataSetWriterUpdateRequestModel {
                        GenerationId = result2.GenerationId,
                        KeyFrameInterval = TimeSpan.FromSeconds(5),
                        WriterGroupId = "noid",
                        DataSetFieldContentMask =
                            Models.DataSetFieldContentMask.ApplicationUri |
                            Models.DataSetFieldContentMask.DisplayName,
                        DataSetName = "supername",
                        ExtensionFields = new Dictionary<string, string> {
                            ["test"] = "total"
                        },
                        KeyFrameCount = 566,
                        MessageSettings = new DataSetWriterMessageSettingsModel {
                            ConfiguredSize = 5,
                            DataSetMessageContentMask =
                                DataSetContentMask.DataSetWriterId |
                                DataSetContentMask.MajorVersion,
                            DataSetOffset = 5,
                            NetworkMessageNumber = 66
                        },
                        SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                            LifeTimeCount = 55,
                            MaxKeepAliveCount = 6,
                            MaxNotificationsPerPublish = 5,
                            Priority = 6,
                            PublishingInterval = TimeSpan.FromMinutes(6),
                            ResolveDisplayName = true
                        },
                        User = new CredentialModel {
                            Type = CredentialType.UserName,
                            Value = "test"
                        }
                    });

                var writer = await service.GetDataSetWriterAsync(result2.DataSetWriterId);
                Assert.Equal(TimeSpan.FromSeconds(5), writer.KeyFrameInterval);
                Assert.Equal(Models.DataSetFieldContentMask.ApplicationUri |
                    Models.DataSetFieldContentMask.DisplayName, writer.DataSetFieldContentMask);
                Assert.Equal(566u, writer.KeyFrameCount);
                Assert.NotNull(writer.DataSet);
                Assert.Equal("supername", writer.DataSet.Name);
                Assert.Equal("total", writer.DataSet.ExtensionFields["test"]);
                Assert.NotNull(writer.DataSet.DataSetSource);
                Assert.NotNull(writer.DataSet.DataSetSource.SubscriptionSettings);
                Assert.Equal(true, writer.DataSet.DataSetSource.SubscriptionSettings.ResolveDisplayName);
                Assert.Equal(TimeSpan.FromMinutes(6), writer.DataSet.DataSetSource.SubscriptionSettings.PublishingInterval);
                Assert.Equal((byte)6, writer.DataSet.DataSetSource.SubscriptionSettings.Priority);
                Assert.Equal(5u, writer.DataSet.DataSetSource.SubscriptionSettings.MaxNotificationsPerPublish);
                Assert.Equal(6u, writer.DataSet.DataSetSource.SubscriptionSettings.MaxKeepAliveCount);
                Assert.Equal(55u, writer.DataSet.DataSetSource.SubscriptionSettings.LifeTimeCount);
                Assert.NotNull(writer.DataSet.DataSetSource.Connection);
                Assert.NotNull(writer.DataSet.DataSetSource.Connection.User);
                Assert.Equal(CredentialType.UserName, writer.DataSet.DataSetSource.Connection.User.Type);
                Assert.Equal("test", writer.DataSet.DataSetSource.Connection.User.Value);
                Assert.NotNull(writer.MessageSettings);
                Assert.Equal(DataSetContentMask.DataSetWriterId | DataSetContentMask.MajorVersion,
                    writer.MessageSettings.DataSetMessageContentMask);
                Assert.Equal((ushort)66, writer.MessageSettings.NetworkMessageNumber);
                Assert.Equal((ushort)5, writer.MessageSettings.ConfiguredSize);
                Assert.Equal((ushort)5, writer.MessageSettings.DataSetOffset);

                await service.UpdateDataSetWriterAsync(result2.DataSetWriterId,
                    new DataSetWriterUpdateRequestModel {
                        GenerationId = writer.GenerationId,
                        KeyFrameInterval = TimeSpan.FromSeconds(0),
                        WriterGroupId = "",
                        DataSetFieldContentMask = 0,
                        DataSetName = "",
                        ExtensionFields = new Dictionary<string, string>(),
                        KeyFrameCount = 0,
                        MessageSettings = new DataSetWriterMessageSettingsModel {
                            ConfiguredSize = 0,
                            DataSetMessageContentMask = 0,
                            DataSetOffset = 0,
                            NetworkMessageNumber = 0
                        },
                        SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                            LifeTimeCount = 0,
                            MaxKeepAliveCount = 0,
                            MaxNotificationsPerPublish = 0,
                            Priority = 0,
                            PublishingInterval = TimeSpan.FromMinutes(0),
                            ResolveDisplayName = false
                        },
                        User = new CredentialModel { }
                    });

                // Assert
                writer = await service.GetDataSetWriterAsync(result2.DataSetWriterId);
                Assert.Null(writer.KeyFrameInterval);
                Assert.Null(writer.DataSetFieldContentMask);
                Assert.Null(writer.KeyFrameCount);
                Assert.NotNull(writer.DataSet);
                Assert.Null(writer.DataSet.Name);
                Assert.Null(writer.DataSet.ExtensionFields);
                Assert.NotNull(writer.DataSet.DataSetSource);
                Assert.NotNull(writer.DataSet.DataSetSource.SubscriptionSettings);
                Assert.Null(writer.DataSet.DataSetSource.SubscriptionSettings.ResolveDisplayName);
                Assert.Null(writer.DataSet.DataSetSource.SubscriptionSettings.PublishingInterval);
                Assert.Null(writer.DataSet.DataSetSource.SubscriptionSettings.Priority);
                Assert.Null(writer.DataSet.DataSetSource.SubscriptionSettings.MaxNotificationsPerPublish);
                Assert.Null(writer.DataSet.DataSetSource.SubscriptionSettings.MaxKeepAliveCount);
                Assert.Null(writer.DataSet.DataSetSource.SubscriptionSettings.LifeTimeCount);
                Assert.NotNull(writer.DataSet.DataSetSource.Connection);
                Assert.Null(writer.DataSet.DataSetSource.Connection.User);
                Assert.NotNull(writer.MessageSettings);
                Assert.Null(writer.MessageSettings.DataSetMessageContentMask);
                Assert.Null(writer.MessageSettings.NetworkMessageNumber);
                Assert.Null(writer.MessageSettings.ConfiguredSize);
                Assert.Null(writer.MessageSettings.DataSetOffset);

                var foundGroups = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Disabled
                });
                Assert.Single(foundGroups);

                // Act
                await groups.ActivateWriterGroupAsync(group.WriterGroupId);
                foundGroups = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Disabled
                });
                Assert.Empty(foundGroups);
                foundGroups = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Pending
                });
                Assert.Single(foundGroups);
                await groups.DeactivateWriterGroupAsync(group.WriterGroupId);
                foundGroups = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Disabled
                });
                Assert.Single(foundGroups);
                foundGroups = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Pending
                });
                Assert.Empty(foundGroups);

                // Act
                await groups.UpdateWriterGroupAsync(group.WriterGroupId,
                    new WriterGroupUpdateRequestModel {
                        GenerationId = group.GenerationId,
                        BatchSize = 0,
                        HeaderLayoutUri = "",
                        KeepAliveTime = TimeSpan.FromSeconds(0),
                        LocaleIds = new List<string>(),
                        MessageSettings = new WriterGroupMessageSettingsModel {
                            DataSetOrdering = 0,
                            GroupVersion = 0u,
                            NetworkMessageContentMask = 0,
                            PublishingOffset = new List<double>(),
                            SamplingOffset = 0.0
                        },
                        Encoding = 0,
                        Schema = 0,
                        Name = "",
                        Priority = 0,
                        PublishingInterval = TimeSpan.FromMilliseconds(0)
                    });

                group = await groups.GetWriterGroupAsync(result1.WriterGroupId);
                Assert.Null(group.BatchSize);
                Assert.Null(group.HeaderLayoutUri);
                Assert.Null(group.KeepAliveTime);
                Assert.Null(group.LocaleIds);
                Assert.NotNull(group.MessageSettings);
                Assert.Null(group.MessageSettings.DataSetOrdering);
                Assert.Null(group.MessageSettings.GroupVersion);
                Assert.Null(group.MessageSettings.NetworkMessageContentMask);
                Assert.Null(group.MessageSettings.PublishingOffset);
                Assert.Null(group.MessageSettings.SamplingOffset);
                Assert.Null(group.Encoding);
                Assert.Null(group.Schema);
                Assert.Null(group.Name);
                Assert.Null(group.Priority);
                Assert.Null(group.PublishingInterval);
            }
        }

        [Fact]
        public async Task ActivateDeactivateGroupTestAsync() {

            using (var mock = Setup((v, q) => {
                var expected =
                    "SELECT * FROM r WHERE r.LastState = 'Pending' AND r.ClassType = 'WriterGroup'";
                if (q == expected) {
                    // Get variables and entities
                    return v
                        .Where(o => o.Value["LastState"] == "Pending")
                        .Where(o => o.Value["ClassType"] == "WriterGroup");
                }

                expected =
                    "SELECT * FROM r WHERE r.LastState = 'Disabled' AND r.ClassType = 'WriterGroup'";
                if (q == expected) {
                    // Get variables and entities
                    return v
                        .Where(o => o.Value["LastState"] == "Disabled")
                        .Where(o => o.Value["ClassType"] == "WriterGroup");
                }

                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();

                // Act
                var result1 = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "Test",
                    SiteId = "fakesite" // See below
                });

                var foundGroups = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Disabled
                });
                Assert.Single(foundGroups);

                // Act
                await groups.ActivateWriterGroupAsync(result1.WriterGroupId);
                foundGroups = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Disabled
                });
                Assert.Empty(foundGroups);
                foundGroups = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Pending
                });
                Assert.Single(foundGroups);
                await groups.DeactivateWriterGroupAsync(result1.WriterGroupId);
                foundGroups = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Disabled
                });
                Assert.Single(foundGroups);
                foundGroups = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Pending
                });
                Assert.Empty(foundGroups);
            }
        }

        [Fact]
        public async Task AddVariablesToDefaultDataSetWriterTestAsync() {

            var dataSetWriterId = "endpoint1";
            var writerGroupId = "fakesite";
            using (var mock = Setup((v, q) => {
                var expected = $"SELECT * FROM r WHERE r.DataSetWriterId = '{dataSetWriterId}' AND " +
                    "r.ClassType = 'DataSetEntity' AND r.Type = 'Variable'";
                if (expected == q) {
                    // Get variables
                    return v
                        .Where(o => o.Value["DataSetWriterId"] == dataSetWriterId)
                        .Where(o => o.Value["ClassType"] == "DataSetEntity")
                        .Where(o => o.Value["Type"] == "Variable");
                }

                expected = $"SELECT * FROM r WHERE r.WriterGroupId = '{writerGroupId}' AND " +
                    "r.IsDisabled = false AND r.ClassType = 'DataSetWriter'";
                if (expected == q) {
                    // Get variables
                    return v
                        .Where(o => o.Value["WriterGroupId"] == writerGroupId)
                        .Where(o => o.Value["ClassType"] == "DataSetWriter")
                        .Where(o => o.Value["IsDisabled"] == false);
                }

                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IDataSetBatchOperations batch = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();

                var result = await batch.AddVariablesToDefaultDataSetWriterAsync(dataSetWriterId,
                    new DataSetAddVariableBatchRequestModel {
                        DataSetPublishingInterval = TimeSpan.FromSeconds(1),
                        Variables = LinqEx.Repeat(() => new DataSetAddVariableRequestModel {
                            PublishedVariableNodeId = "i=2554",
                            HeartbeatInterval = TimeSpan.FromDays(1)
                        }, 10).ToList()
                    });

                var writer = await service.GetDataSetWriterAsync(dataSetWriterId);

                // Assert
                Assert.NotNull(writer);
                Assert.Equal(dataSetWriterId, writer.DataSetWriterId);
                Assert.NotNull(writer.DataSet);
                Assert.NotNull(writer.DataSet.DataSetSource);
                Assert.NotNull(writer.DataSet.DataSetSource.Connection);
                Assert.NotNull(writer.DataSet.DataSetSource.Connection.Endpoint);
                Assert.Equal("fakeurl", writer.DataSet.DataSetSource.Connection.Endpoint.Url);
                Assert.NotNull(writer.DataSet.DataSetSource.SubscriptionSettings);
                Assert.Equal(TimeSpan.FromSeconds(1), writer.DataSet.DataSetSource.SubscriptionSettings.PublishingInterval);

                // Act
                var found = await service.ListAllDataSetVariablesAsync(writer.DataSetWriterId);

                // Assert
                Assert.Single(found);
                Assert.Equal("i=2554", found.Single().PublishedVariableNodeId);
                Assert.Null(found.Single().PublishedVariableDisplayName);
                Assert.Equal(TimeSpan.FromDays(1), found.Single().HeartbeatInterval);
                Assert.Contains(result.Results, f => f.Id == found.Single().Id);

                // Act
                var group = await groups.GetWriterGroupAsync(writerGroupId);

                // Assert

                Assert.NotNull(group);
                Assert.Equal(writerGroupId, group.WriterGroupId);
                Assert.Equal("fakesite", group.SiteId);
                Assert.Single(group.DataSetWriters);
                Assert.Collection(group.DataSetWriters, writer2 => {
                    Assert.Equal(writer.DataSetWriterId, writer2.DataSetWriterId);
                    Assert.Equal("fakeurl", writer2.DataSet.DataSetSource.Connection.Endpoint.Url);
                });

                // Act
                result = await batch.AddVariablesToDefaultDataSetWriterAsync(dataSetWriterId,
                    new DataSetAddVariableBatchRequestModel {
                        DataSetPublishingInterval = TimeSpan.FromSeconds(2),
                        Variables = LinqEx.Repeat(() => new DataSetAddVariableRequestModel {
                            PublishedVariableNodeId = "i=2553",
                            HeartbeatInterval = TimeSpan.FromDays(3)
                        }, 10).ToList()
                    });

                writer = await service.GetDataSetWriterAsync(dataSetWriterId);

                // Assert
                Assert.NotNull(writer);
                Assert.Equal(dataSetWriterId, writer.DataSetWriterId);
                Assert.NotNull(writer.DataSet);
                Assert.NotNull(writer.DataSet.DataSetSource);
                Assert.NotNull(writer.DataSet.DataSetSource.Connection);
                Assert.NotNull(writer.DataSet.DataSetSource.Connection.Endpoint);
                Assert.Equal("fakeurl", writer.DataSet.DataSetSource.Connection.Endpoint.Url);
                Assert.NotNull(writer.DataSet.DataSetSource.SubscriptionSettings);
                Assert.Equal(TimeSpan.FromSeconds(2), writer.DataSet.DataSetSource.SubscriptionSettings.PublishingInterval);

                // Act
                found = await service.ListAllDataSetVariablesAsync(writer.DataSetWriterId);

                // Assert
                Assert.Equal(2, found.Count);
            }
        }


        /// <summary>
        /// Setup mock
        /// </summary>
        /// <param name="mock"></param>
        /// <param name="provider"></param>
        private static AutoMock Setup(Func<IEnumerable<IDocumentInfo<VariantValue>>,
            string, IEnumerable<IDocumentInfo<VariantValue>>> provider) {
            var mock = AutoMock.GetLoose(builder => {
                builder.RegisterType<NewtonSoftJsonConverters>().As<IJsonSerializerConverterProvider>();
                builder.RegisterType<NewtonSoftJsonSerializer>().As<IJsonSerializer>();
                builder.RegisterInstance(new QueryEngineAdapter(provider)).As<IQueryEngine>();
                builder.RegisterType<MemoryDatabase>().SingleInstance().As<IDatabaseServer>();
                builder.RegisterType<ItemContainerFactory>().As<IItemContainerFactory>();
                builder.RegisterType<DataSetEntityDatabase>().AsImplementedInterfaces();
                builder.RegisterType<DataSetWriterDatabase>().AsImplementedInterfaces();
                builder.RegisterType<WriterGroupDatabase>().AsImplementedInterfaces();
                var registry = new Mock<IEndpointRegistry>();
                registry
                    .Setup(e => e.GetEndpointAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(new EndpointInfoModel {
                        Registration = new EndpointRegistrationModel {
                            EndpointUrl = "fakeurl",
                            Id = "endpoint1",
                            SiteId = "fakesite",
                            Endpoint = new EndpointModel {
                                Url = "fakeurl"
                            }
                        }
                    }));
                builder.RegisterMock(registry);
                builder.RegisterType<WriterGroupRegistry>().AsImplementedInterfaces();
            });

            return mock;
        }
    }
}

