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
    using Xunit;
    using Xunit.Sdk;
    using Autofac;
    using Moq;
    using System.Threading;
    using Opc.Ua;

    /// <summary>
    /// Certificate Issuer tests
    /// </summary>
    public class WriterGroupRegistryTests {

        [Fact]
        public async Task AddWriterButNoGroupExistsShouldExceptAsync() {

            using (var mock = Setup((v, q) => {
                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IWriterGroupRepository store = mock.Create<WriterGroupDatabase>();

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
                IWriterGroupRepository store = mock.Create<WriterGroupDatabase>();

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
                IWriterGroupRepository store = mock.Create<WriterGroupDatabase>();

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
                IWriterGroupRepository store = mock.Create<WriterGroupDatabase>();

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
                IWriterGroupRepository store = mock.Create<WriterGroupDatabase>();

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
                IWriterGroupRepository store = mock.Create<WriterGroupDatabase>();

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
                IWriterGroupRepository store = mock.Create<WriterGroupDatabase>();

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
                IDataSetBatchOperations batch = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();
                IWriterGroupRepository store = mock.Create<WriterGroupDatabase>();

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
                    Assert.Null(v.PublishedVariableDisplayName);
                    Assert.Equal(TimeSpan.FromDays(1), v.HeartbeatInterval);
                    Assert.Contains(result.Results, f => f.Id == v.Id);
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
                IWriterGroupRepository store = mock.Create<WriterGroupDatabase>();

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

