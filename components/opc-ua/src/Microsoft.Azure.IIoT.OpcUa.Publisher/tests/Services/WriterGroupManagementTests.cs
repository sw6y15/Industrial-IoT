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
    /// Writer group management tests
    /// </summary>
    public class WriterGroupManagementTests {

        [Fact]
        public async Task UpdateWriterGroupStateTestAsync() {

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

                expected =
                    "SELECT * FROM r WHERE r.LastState = 'Publishing' AND r.ClassType = 'WriterGroup'";
                if (q == expected) {
                    // Get variables and entities
                    return v
                        .Where(o => o.Value["LastState"] == "Publishing")
                        .Where(o => o.Value["ClassType"] == "WriterGroup");
                }
                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry writers = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();
                IWriterGroupStateUpdate service = mock.Create<WriterGroupManagement>();

                // Act
                var result1 = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "Test",
                    SiteId = "fakesite" // See below
                });

                // Assert
                var found = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Disabled
                });
                Assert.Single(found); // Initial state is disabled

                // Act
                await service.UpdateWriterGroupStateAsync(result1.WriterGroupId, WriterGroupState.Publishing);
                // Assert
                found = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Publishing
                });
                Assert.Empty(found); // No publishing if not activate

                // Act
                await groups.ActivateWriterGroupAsync(result1.WriterGroupId);
                await service.UpdateWriterGroupStateAsync(result1.WriterGroupId, WriterGroupState.Publishing);
                found = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Pending
                });
                Assert.Empty(found);
                found = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Publishing
                });
                Assert.Single(found); // Publishing - not pending

                // Act
                await service.UpdateWriterGroupStateAsync(result1.WriterGroupId, WriterGroupState.Publishing);
                found = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Publishing
                });
                Assert.Single(found);
                await service.UpdateWriterGroupStateAsync(result1.WriterGroupId, WriterGroupState.Pending);
                found = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Pending
                });
                Assert.Single(found);
                await service.UpdateWriterGroupStateAsync(result1.WriterGroupId, WriterGroupState.Publishing);
                found = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Publishing
                });
                Assert.Single(found);

                // Act
                await groups.DeactivateWriterGroupAsync(result1.WriterGroupId);
                found = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Disabled
                });
                Assert.Single(found);

                await service.UpdateWriterGroupStateAsync(result1.WriterGroupId, WriterGroupState.Publishing);

                found = await groups.QueryAllWriterGroupsAsync(new WriterGroupInfoQueryModel {
                    State = WriterGroupState.Disabled
                });
                Assert.Single(found); // No publishing if disabled
            }
        }

        [Fact]
        public async Task UpdateDataSetWriterStateTestAsync() {

            using (var mock = Setup((v, q) => {
                var expected =
                    "SELECT * FROM r WHERE r.IsDisabled = false AND r.ClassType = 'DataSetWriter'";
                if (q == expected) {
                    // Get variables and entities
                    return v
                        .Where(o => o.Value["IsDisabled"] == false)
                        .Where(o => o.Value["ClassType"] == "DataSetWriter");
                }
                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry writers = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();
                IDataSetWriterStateUpdate service = mock.Create<WriterGroupManagement>();

                var result1 = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "Test",
                    SiteId = "fakesite" // See below
                });

                var result2 = await writers.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                    EndpointId = "endpoint1",
                    DataSetName = "Test",
                    KeyFrameInterval = TimeSpan.FromSeconds(1),
                    SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                        Priority = 1
                    },
                    WriterGroupId = result1.WriterGroupId
                });

                // Assert
                var found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Single(found); // Initial state is enabled

                // Act
                var now = DateTime.UtcNow;
                await service.UpdateDataSetWriterStateAsync(result2.DataSetWriterId,
                    new PublishedDataSetSourceStateModel {
                        LastResult = new ServiceResultModel {
                            ErrorMessage = "error"
                        },
                        LastResultChange = now
                    });

                // Assert
                found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Single(found);
                Assert.Equal("error", found.Single().DataSet.State.LastResult.ErrorMessage);
                Assert.Equal(now, found.Single().DataSet.State.LastResultChange);


                // Act
                now = DateTime.UtcNow;
                await service.UpdateDataSetWriterStateAsync(result2.DataSetWriterId,
                    new PublishedDataSetSourceStateModel {
                        LastResultChange = now
                    });

                // Assert
                found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Single(found);
                Assert.Null(found.Single().DataSet.State.LastResult);
                Assert.Equal(now, found.Single().DataSet.State.LastResultChange);
            }
        }

        [Fact]
        public async Task UpdateDataSetVariableStateTestAsync() {

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
                var expected =
                    "SELECT * FROM r WHERE r.IsDisabled = false AND r.ClassType = 'DataSetWriter'";
                if (q == expected) {
                    // Get variables and entities
                    return v
                        .Where(o => o.Value["IsDisabled"] == false)
                        .Where(o => o.Value["ClassType"] == "DataSetWriter");
                }
                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry writers = mock.Create<WriterGroupRegistry>();
                IDataSetBatchOperations batch = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();
                IDataSetWriterStateUpdate service = mock.Create<WriterGroupManagement>();

                var result1 = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "Test",
                    SiteId = "fakesite" // See below
                });

                var result2 = await writers.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                    EndpointId = "endpoint1",
                    DataSetName = "Test",
                    KeyFrameInterval = TimeSpan.FromSeconds(1),
                    SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                        Priority = 1
                    },
                    WriterGroupId = result1.WriterGroupId
                });

                var result = await batch.AddVariablesToDataSetWriterAsync(result2.DataSetWriterId,
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

                var found = await writers.ListAllDataSetVariablesAsync(result2.DataSetWriterId);
                Assert.Equal(3, found.Count);
                var v = found.First();
                var targetId = v.Id;
                var now = DateTime.UtcNow;
                Assert.NotNull(v);
                Assert.Null(v.State);

                await service.UpdateDataSetVariableStateAsync(result2.DataSetWriterId, targetId,
                    new PublishedDataSetItemStateModel {
                        ClientId = 444,
                        LastResult = new ServiceResultModel {
                            StatusCode = 55
                        },
                        ServerId = 5,
                        LastResultChange = now
                    });

                found = await writers.ListAllDataSetVariablesAsync(result2.DataSetWriterId);
                v = found.FirstOrDefault(v => v.Id == targetId);
                Assert.NotNull(v);
                Assert.NotNull(v.State);
                Assert.Equal(444u, v.State.ClientId);
                Assert.Equal(5u, v.State.ServerId);
                Assert.NotNull(v.State.LastResult);
                Assert.Equal(55u, v.State.LastResult.StatusCode);
                Assert.Equal(now, v.State.LastResultChange);

                await service.UpdateDataSetVariableStateAsync(result2.DataSetWriterId, targetId,
                    new PublishedDataSetItemStateModel {
                        ClientId = 0,
                        ServerId = 0,
                        LastResultChange = now
                    });

                found = await writers.ListAllDataSetVariablesAsync(result2.DataSetWriterId);
                v = found.FirstOrDefault(v => v.Id == targetId);
                Assert.NotNull(v);
                Assert.NotNull(v.State);
                Assert.Null(v.State.ClientId);
                Assert.Null(v.State.ServerId);
                Assert.Null(v.State.LastResult);
                Assert.Equal(now, v.State.LastResultChange);
            }
        }

        [Fact]
        public async Task HandleEndpointEventsTestAsync() {

            using (var mock = Setup((v, q) => {
                var expected =
                    "SELECT * FROM r WHERE r.IsDisabled = false AND r.ClassType = 'DataSetWriter'";
                if (q == expected) {
                    return v
                        .Where(o => o.Value["IsDisabled"] == false)
                        .Where(o => o.Value["ClassType"] == "DataSetWriter");
                }
                expected =
                    "SELECT * FROM r WHERE r.ClassType = 'DataSetWriter'";
                if (q == expected) {
                    return v
                        .Where(o => o.Value["ClassType"] == "DataSetWriter");
                }
                expected =
                    "SELECT * FROM r WHERE r.EndpointId = 'endpoint1' AND r.ClassType = 'DataSetWriter'";
                if (q == expected) {
                    return v
                        .Where(o => o.Value["EndpointId"] == "endpoint1")
                        .Where(o => o.Value["ClassType"] == "DataSetWriter");
                }
                expected =
                    "SELECT * FROM r WHERE r.EndpointId = 'endpoint1' AND r.IsDisabled = false AND r.ClassType = 'DataSetWriter'";
                if (q == expected) {
                    return v
                        .Where(o => o.Value["IsDisabled"] == false)
                        .Where(o => o.Value["EndpointId"] == "endpoint1")
                        .Where(o => o.Value["ClassType"] == "DataSetWriter");
                }
                throw new AssertActualExpectedException(null, q, "Query");
            })) {

                IDataSetWriterRegistry writers = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();
                IEndpointRegistryListener service = mock.Create<WriterGroupManagement>();

                var endpoint = new EndpointInfoModel {
                    Registration = new EndpointRegistrationModel {
                        EndpointUrl = "fakeurl",
                        Id = "endpoint1",
                        SiteId = "fakesite",
                        Endpoint = new EndpointModel {
                            Url = "fakeurl"
                        }
                    }
                };

                var result1 = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "Test",
                    SiteId = "fakesite" // See below
                });

                var result2 = await writers.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                    EndpointId = "endpoint1",
                    DataSetName = "Test",
                    KeyFrameInterval = TimeSpan.FromSeconds(1),
                    SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                        Priority = 1
                    },
                    WriterGroupId = result1.WriterGroupId
                });

                // Assert
                var found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Single(found);

                // Act
                await service.OnEndpointActivatedAsync(null, endpoint);
                // Assert
                found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Single(found);

                // Act
                await service.OnEndpointDeactivatedAsync(null, endpoint);
                // Assert
                found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Empty(found);

                // Act
                await service.OnEndpointDeactivatedAsync(null, endpoint);
                // Assert
                found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Empty(found);

                // Act
                await service.OnEndpointActivatedAsync(null, endpoint);
                // Assert
                found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Single(found);

                // Act
                await service.OnEndpointEnabledAsync(null, endpoint);
                // Assert
                found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Single(found);

                // Act
                await service.OnEndpointDisabledAsync(null, endpoint);
                // Assert
                found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Empty(found);

                // Act
                await service.OnEndpointEnabledAsync(null, endpoint);
                // Assert
                found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Single(found);

                // Act
                await service.OnEndpointDeletedAsync(null, endpoint.Registration.Id, null);
                // Assert
                found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Empty(found);

                // Act
                await service.OnEndpointDeletedAsync(null, endpoint.Registration.Id, null);
                // Assert
                found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Empty(found);

                // Act
                await service.OnEndpointNewAsync(null, endpoint);
                // Assert
                found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Single(found);

                // Act
                await service.OnEndpointDeletedAsync(null, endpoint.Registration.Id, null);
                // Assert
                found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Empty(found);

                // Act
                await service.OnEndpointActivatedAsync(null, endpoint);
                // Assert
                found = await writers.QueryAllDataSetWritersAsync(new DataSetWriterInfoQueryModel {
                    ExcludeDisabled = true
                });
                Assert.Single(found);
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
                builder.RegisterType<WriterGroupManagement>().AsImplementedInterfaces();
            });

            return mock;
        }
    }
}

