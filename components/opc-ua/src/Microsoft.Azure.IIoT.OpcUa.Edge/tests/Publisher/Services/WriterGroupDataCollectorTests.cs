// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Services {
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Services;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Storage.Default;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Default;
    using Microsoft.Azure.IIoT.OpcUa.Protocol.Services;
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher;
    using Microsoft.Azure.IIoT.OpcUa.Registry;
    using Microsoft.Azure.IIoT.OpcUa.Registry.Models;
    using Microsoft.Azure.IIoT.OpcUa.Testing.Fixtures;
    using Microsoft.Azure.IIoT.OpcUa.Protocol.Runtime;
    using Microsoft.Azure.IIoT.Serializers;
    using Microsoft.Azure.IIoT.Serializers.NewtonSoft;
    using Microsoft.Azure.IIoT.Storage;
    using Microsoft.Azure.IIoT.Storage.Default;
    using Microsoft.Azure.IIoT.Utils;
    using Microsoft.Extensions.Configuration;
    using Autofac;
    using Autofac.Extras.Moq;
    using Moq;
    using System;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Linq;
    using System.Net.Sockets;
    using Xunit;
    using Xunit.Sdk;
    using Serilog;
    using Opc.Ua;

    [Collection(PublishCollection.Name)]
    public class WriterGroupDataCollectorTests {

        public WriterGroupDataCollectorTests(TestServerFixture server) {
            _server = server;
            _hostEntry = Try.Op(() => Dns.GetHostEntry(Opc.Ua.Utils.GetHostName()))
                ?? Try.Op(() => Dns.GetHostEntry("localhost"));
        }

        [Fact]
        public async Task WriterGroupSetupPublishingTestAsync() {

            using (var mock = Setup()) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();
                var events = mock.Create<ObservableEventFixture>();

                // Act
                var result1 = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "TestGroup",
                    SiteId = "fakesite" // See below
                });

                // Add a single writer to endpoint
                var result2 = await service.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                    EndpointId = "endpoint1", // See below
                    DataSetName = "TestSet",
                    SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                        PublishingInterval = TimeSpan.FromSeconds(1)
                    },
                    WriterGroupId = result1.WriterGroupId
                });

                var variable = await service.AddDataSetVariableAsync(
                    result2.DataSetWriterId,
                    new DataSetAddVariableRequestModel {
                        PublishedVariableNodeId = "i=2258", // server time
                        SamplingInterval = TimeSpan.FromSeconds(1)
                    });

                // Activate the group - will start the engine
                await groups.ActivateWriterGroupAsync(result1.WriterGroupId);

                // Should get a good source state
                var sevt = events.GetSourceStates(result2.DataSetWriterId).WaitForEvent();
                Assert.NotNull(sevt);
                Assert.Null(sevt.LastResult?.ErrorMessage);
                Assert.Null(sevt.LastResult?.StatusCode);

                // Should get state change for item
                var v1evt = events.GetItemStates(result2.DataSetWriterId, variable.Id)
                    .WaitForEvent(e => e.ServerId != null);
                Assert.NotNull(v1evt);
                Assert.Null(v1evt.LastResult?.ErrorMessage);
                Assert.Null(v1evt.LastResult?.StatusCode);

                // Should get messages
                var message = events.GetMessages(result1.WriterGroupId).WaitForEvent();
                Assert.NotNull(message);
                Assert.NotNull(message.Data);
                Assert.NotNull(message.ContentEncoding);
                Assert.Equal(ContentMimeType.Json, message.ContentType);
                var value = message.Decode();
                Assert.False(value.IsNull());
                Assert.Equal("1", value.GetByPath("MessageId"));
                Assert.Equal("ua-data", value.GetByPath("MessageType"));
                Assert.Equal(1, value.GetByPath("Messages[0].MetaDataVersion.MajorVersion"));
                Assert.True(value.GetByPath("Messages[0].Status").IsNull());
                Assert.True(value.GetByPath("Messages[0].Payload.i=2258.Value").IsDateTime);

                message = events.GetMessages(result1.WriterGroupId).WaitForEvent();
                Assert.NotNull(message);
                Assert.NotNull(message.Data);
                Assert.NotNull(message.ContentEncoding);
                Assert.Equal(ContentMimeType.Json, message.ContentType);
                value = message.Decode();
                Assert.False(value.IsNull());
                Assert.Equal("2", value.GetByPath("MessageId"));
                Assert.Equal("ua-data", value.GetByPath("MessageType"));
                Assert.Equal(1, value.GetByPath("Messages[0].MetaDataVersion.MajorVersion"));
                Assert.True(value.GetByPath("Messages[0].Status").IsNull());
                Assert.True(value.GetByPath("Messages[0].Payload.i=2258.Value").IsDateTime);

                // Deactivate - stop engine
                await groups.DeactivateWriterGroupAsync(result1.WriterGroupId);
            }
        }

        [Fact]
        public async Task WriterGroupSetupPublishingTestWithBadNodeAsync() {

            using (var mock = Setup()) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();
                var events = mock.Create<ObservableEventFixture>();

                // Act
                var result1 = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "TestGroup",
                    SiteId = "fakesite" // See below
                });

                // Add a single writer to endpoint
                var result2 = await service.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                    EndpointId = "endpoint1", // See below
                    DataSetName = "TestSet",
                    SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                        PublishingInterval = TimeSpan.FromSeconds(1)
                    },
                    WriterGroupId = result1.WriterGroupId
                });

                var variable = await service.AddDataSetVariableAsync(
                    result2.DataSetWriterId,
                    new DataSetAddVariableRequestModel {
                        PublishedVariableNodeId = "i=88888", // bad
                        SamplingInterval = TimeSpan.FromSeconds(1)
                    });

                // Activate the group - will start the engine
                await groups.ActivateWriterGroupAsync(result1.WriterGroupId);

                // Should get a good source state
                var sevt = events.GetSourceStates(result2.DataSetWriterId).WaitForEvent();
                Assert.NotNull(sevt);
                Assert.Null(sevt.LastResult?.ErrorMessage);
                Assert.Null(sevt.LastResult?.StatusCode);

                // Should get BadNodeIdInvalid state change for item
                var v1evt = events.GetItemStates(result2.DataSetWriterId, variable.Id)
                    .WaitForEvent(e => e.LastResult?.StatusCode != null);
                Assert.NotNull(v1evt);
                Assert.Equal(StatusCodes.BadNodeIdUnknown, v1evt.LastResult?.StatusCode);
                Assert.NotNull(v1evt.LastResult?.ErrorMessage);

                // Should get single message with error
                var message = events.GetMessages(result1.WriterGroupId).WaitForEvent();
                Assert.NotNull(message.Data);
                Assert.NotNull(message.ContentEncoding);
                Assert.Equal(ContentMimeType.Json, message.ContentType);
                Assert.NotNull(message);
                var value = message.Decode();
                Assert.False(value.IsNull());
                Assert.Equal("1", value.GetByPath("MessageId"));
                Assert.Equal("ua-data", value.GetByPath("MessageType"));
                Assert.Equal(1, value.GetByPath("Messages[0].MetaDataVersion.MajorVersion"));
                Assert.Equal("Bad", value.GetByPath("Messages[0].Status.Symbol"));
                Assert.Equal("BadNodeIdUnknown", value.GetByPath("Messages[0].Payload.i=88888.StatusCode.Symbol"));

                // Deactivate - stop engine
                await groups.DeactivateWriterGroupAsync(result1.WriterGroupId);
            }
        }

        [Fact]
        public async Task WriterGroupSetupPublishingAddRemoveVariableAsync() {

            using (var mock = Setup()) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();
                var events = mock.Create<ObservableEventFixture>();

                // Act
                var result1 = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "TestGroup",
                    SiteId = "fakesite" // See below
                });

                // Add a single writer to endpoint
                var result2 = await service.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                    EndpointId = "endpoint1", // See below
                    DataSetName = "TestSet",
                    SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                        PublishingInterval = TimeSpan.FromSeconds(1)
                    },
                    WriterGroupId = result1.WriterGroupId
                });

                var variable = await service.AddDataSetVariableAsync(
                    result2.DataSetWriterId,
                    new DataSetAddVariableRequestModel {
                        PublishedVariableNodeId = "i=88888", // bad
                        SamplingInterval = TimeSpan.FromSeconds(1)
                    });

                // Activate the group - will start the engine
                await groups.ActivateWriterGroupAsync(result1.WriterGroupId);

                // Should get a good source state
                var sevt = events.GetSourceStates(result2.DataSetWriterId).WaitForEvent();
                Assert.NotNull(sevt);
                Assert.Null(sevt.LastResult?.ErrorMessage);
                Assert.Null(sevt.LastResult?.StatusCode);

                // Should get BadNodeIdInvalid state change for item
                var v1evt = events.GetItemStates(result2.DataSetWriterId, variable.Id)
                    .WaitForEvent(e => e.LastResult?.StatusCode != null);
                Assert.NotNull(v1evt);
                Assert.Equal(StatusCodes.BadNodeIdUnknown, v1evt.LastResult?.StatusCode);
                Assert.NotNull(v1evt.LastResult?.ErrorMessage);

                // Add a single writer to endpoint
                variable = await service.AddDataSetVariableAsync(
                    result2.DataSetWriterId,
                    new DataSetAddVariableRequestModel {
                        PublishedVariableNodeId = "i=2258", // good
                        SamplingInterval = TimeSpan.FromSeconds(1)
                    });

                // Should get state change for item
                var v2evt = events.GetItemStates(result2.DataSetWriterId, variable.Id)
                    .WaitForEvent(e => e.ServerId != null);
                Assert.NotNull(v2evt);
                Assert.Null(v2evt.LastResult?.ErrorMessage);
                Assert.Null(v2evt.LastResult?.StatusCode);

                // Should get messages
                var message = events.GetMessages(result1.WriterGroupId).WaitForEvent();
                Assert.NotNull(message);
                Assert.NotNull(message.Data);
                Assert.NotNull(message.ContentEncoding);
                Assert.Equal(ContentMimeType.Json, message.ContentType);

                // Deactivate - stop engine
                await groups.DeactivateWriterGroupAsync(result1.WriterGroupId);
            }
        }

        [Fact]
        public async Task WriterGroupSetupPublishingAddGoodAndBadAsync() {

            using (var mock = Setup()) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();
                var events = mock.Create<ObservableEventFixture>();

                // Act
                var result1 = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "TestGroup",
                    SiteId = "fakesite" // See below
                });

                // Add a single writer to endpoint
                var result2 = await service.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                    EndpointId = "endpoint1", // See below
                    DataSetName = "TestSet",
                    SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                        PublishingInterval = TimeSpan.FromSeconds(1)
                    },
                    WriterGroupId = result1.WriterGroupId
                });

                var bad = await service.AddDataSetVariableAsync(
                    result2.DataSetWriterId,
                    new DataSetAddVariableRequestModel {
                        PublishedVariableNodeId = "i=88888", // bad
                        SamplingInterval = TimeSpan.FromSeconds(1)
                    });

                // Add a single writer to endpoint
                var good = await service.AddDataSetVariableAsync(
                    result2.DataSetWriterId,
                    new DataSetAddVariableRequestModel {
                        PublishedVariableNodeId = "i=2258", // good
                        SamplingInterval = TimeSpan.FromSeconds(1)
                    });

                // Activate the group - will start the engine
                await groups.ActivateWriterGroupAsync(result1.WriterGroupId);

                // Should get a good source state
                var sevt = events.GetSourceStates(result2.DataSetWriterId).WaitForEvent();
                Assert.NotNull(sevt);
                Assert.Null(sevt.LastResult?.ErrorMessage);
                Assert.Null(sevt.LastResult?.StatusCode);

                // Should get BadNodeIdInvalid state change for item
                var v1evt = events.GetItemStates(result2.DataSetWriterId, bad.Id)
                    .WaitForEvent(e => e.LastResult?.StatusCode != null);
                Assert.NotNull(v1evt);
                Assert.Equal(StatusCodes.BadNodeIdUnknown, v1evt.LastResult?.StatusCode);
                Assert.NotNull(v1evt.LastResult?.ErrorMessage);

                // Should get state change for good item
                var v2evt = events.GetItemStates(result2.DataSetWriterId, good.Id)
                    .WaitForEvent(e => e.ServerId != null);
                Assert.NotNull(v2evt);
                Assert.Null(v2evt.LastResult?.ErrorMessage);
                Assert.Null(v2evt.LastResult?.StatusCode);

                // Second message should at least be good
                var message = events.GetMessages(result1.WriterGroupId).WaitForEvent();
                Assert.NotNull(message.Data);
                Assert.Equal(ContentMimeType.Json, message.ContentType);
                Assert.NotNull(message.ContentEncoding);
                Assert.NotNull(message);
                var value = message.Decode();
                Assert.False(value.IsNull());
                Assert.Equal("1", value.GetByPath("MessageId"));
                Assert.Equal("ua-data", value.GetByPath("MessageType"));
                Assert.Equal(1, value.GetByPath("Messages[0].MetaDataVersion.MajorVersion"));
                Assert.Equal("Bad", value.GetByPath("Messages[0].Status.Symbol"));
                Assert.Equal("BadNodeIdUnknown", value.GetByPath("Messages[0].Payload.i=88888.StatusCode.Symbol"));

                // Good
                message = events.GetMessages(result1.WriterGroupId).WaitForEvent();
                Assert.NotNull(message);
                Assert.NotNull(message.Data);
                Assert.NotNull(message.ContentEncoding);
                Assert.Equal(ContentMimeType.Json, message.ContentType);
                value = message.Decode();
                Assert.False(value.IsNull());
                Assert.Equal("2", value.GetByPath("MessageId"));
                Assert.Equal("ua-data", value.GetByPath("MessageType"));
                Assert.Equal(1, value.GetByPath("Messages[0].MetaDataVersion.MajorVersion"));
                Assert.True(value.GetByPath("Messages[0].Status").IsNull());
                Assert.True(value.GetByPath("Messages[0].Payload.i=2258.Value").IsDateTime);

                // Deactivate - stop engine
                await groups.DeactivateWriterGroupAsync(result1.WriterGroupId);
            }
        }

        [Theory]
        [InlineData(5, 0)]
        [InlineData(5, 5)]
        [InlineData(5, 100)]
        [InlineData(100, 5)]
        [InlineData(1000, 5)]
        public async Task WriterGroupSetupPublishingBatchTestAsync(int batchSize, int intervalInSec) {

            using (var mock = Setup()) {

                IDataSetWriterRegistry service = mock.Create<WriterGroupRegistry>();
                IWriterGroupRegistry groups = mock.Create<WriterGroupRegistry>();
                var events = mock.Create<ObservableEventFixture>();

                // Act
                var result1 = await groups.AddWriterGroupAsync(new WriterGroupAddRequestModel {
                    Name = "TestGroup",
                    BatchSize = batchSize,
                    // This is the key - after 5 seconds it should click
                    PublishingInterval = intervalInSec == 0 ?
                        (TimeSpan?)null : TimeSpan.FromSeconds(intervalInSec),
                    SiteId = "fakesite" // See below
                });

                // Add a single writer to endpoint
                var result2 = await service.AddDataSetWriterAsync(new DataSetWriterAddRequestModel {
                    EndpointId = "endpoint1", // See below
                    DataSetName = "TestSet",
                    SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                        PublishingInterval = TimeSpan.FromSeconds(1)
                    },
                    WriterGroupId = result1.WriterGroupId
                });

                var variable = await service.AddDataSetVariableAsync(
                    result2.DataSetWriterId,
                    new DataSetAddVariableRequestModel {
                        PublishedVariableNodeId = "i=2258", // server time
                        SamplingInterval = TimeSpan.FromSeconds(1)
                    });

                // Activate the group - will start the engine
                await groups.ActivateWriterGroupAsync(result1.WriterGroupId);

                // Should get a good source state
                var sevt = events.GetSourceStates(result2.DataSetWriterId).WaitForEvent();
                Assert.NotNull(sevt);
                Assert.Null(sevt.LastResult?.ErrorMessage);
                Assert.Null(sevt.LastResult?.StatusCode);

                // Should get state change for item
                var v1evt = events.GetItemStates(result2.DataSetWriterId, variable.Id)
                    .WaitForEvent(e => e.ServerId != null);
                Assert.NotNull(v1evt);
                Assert.Null(v1evt.LastResult?.ErrorMessage);
                Assert.Null(v1evt.LastResult?.StatusCode);

                // Should get messages
                var message = events.GetMessages(result1.WriterGroupId).WaitForEvent();
                Assert.NotNull(message);
                Assert.NotNull(message.Data);
                Assert.NotNull(message.ContentEncoding);
                Assert.Equal(ContentMimeType.Json, message.ContentType);
                var value = message.Decode();
                Assert.False(value.IsNull());
                Assert.True(value.IsArray);
                Assert.True(value.Count > 0);

                message = events.GetMessages(result1.WriterGroupId).WaitForEvent();
                Assert.NotNull(message);
                value = message.Decode();
                Assert.False(value.IsNull());
                Assert.True(value.IsArray);
                Assert.True(value.Count > 0);

                message = events.GetMessages(result1.WriterGroupId).WaitForEvent();
                Assert.NotNull(message);
                value = message.Decode();
                Assert.False(value.IsNull());
                Assert.True(value.IsArray);
                Assert.True(value.Count > 3);

                // Deactivate - stop engine
                await groups.DeactivateWriterGroupAsync(result1.WriterGroupId);
            }
        }

        /// <summary>
        /// Setup mock
        /// </summary>
        private AutoMock Setup() {
            var mock = AutoMock.GetLoose(builder => {
                builder.RegisterInstance(new ConfigurationBuilder().Build()).AsImplementedInterfaces();
                builder.RegisterInstance(Log.Logger).AsImplementedInterfaces();
                builder.RegisterType<ClientServicesConfig>().AsImplementedInterfaces();
                builder.RegisterType<NewtonSoftJsonConverters>().As<IJsonSerializerConverterProvider>();
                builder.RegisterType<NewtonSoftJsonSerializer>().As<IJsonSerializer>();
                builder.RegisterInstance(new QueryEngineAdapter((v, q) => {
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
                })).As<IQueryEngine>();
                builder.RegisterType<MemoryDatabase>().As<IDatabaseServer>().SingleInstance();
                builder.RegisterType<ItemContainerFactory>().As<IItemContainerFactory>();
                builder.RegisterType<DataSetEntityDatabase>().AsImplementedInterfaces();
                builder.RegisterType<DataSetWriterDatabase>().AsImplementedInterfaces();
                builder.RegisterType<WriterGroupDatabase>().AsImplementedInterfaces();
                var registry = new Mock<IEndpointRegistry>();
                var url = $"opc.tcp://{_hostEntry?.HostName ?? "localhost"}:{_server.Port}/UA/SampleServer";
                registry
                    .Setup(e => e.GetEndpointAsync(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(new EndpointInfoModel {
                        Registration = new EndpointRegistrationModel {
                            EndpointUrl = url,
                            Id = "endpoint1",
                            SiteId = "fakesite",
                            Endpoint = new EndpointModel {
                                Url = url,
                                AlternativeUrls = _hostEntry?.AddressList
                                    .Where(ip => ip.AddressFamily == AddressFamily.InterNetwork)
                                    .Select(ip => $"opc.tcp://{ip}:{_server.Port}/UA/SampleServer").ToHashSet(),
                                Certificate = _server.Certificate?.RawData?.ToThumbprint()
                            }
                        }
                    }));
                builder.RegisterMock(registry);
                builder.RegisterType<DataSetWriterEventBroker>().AsImplementedInterfaces().SingleInstance();
                builder.RegisterType<WriterGroupEventBroker>().AsImplementedInterfaces().SingleInstance();
                builder.RegisterType<WriterGroupRegistry>().AsImplementedInterfaces().SingleInstance();
                builder.RegisterType<WriterGroupManagement>().AsImplementedInterfaces().SingleInstance();
                builder.RegisterType<WriterRegistryConnector>().AsImplementedInterfaces().SingleInstance()
                    .AutoActivate(); // Create and register with broker
                builder.RegisterType<WriterGroupMessageEmitter>().AsImplementedInterfaces().SingleInstance();
                builder.RegisterType<WriterGroupDataCollector>().AsImplementedInterfaces();
                builder.RegisterType<UadpNetworkMessageEncoder>().AsImplementedInterfaces();
                builder.RegisterType<JsonNetworkMessageEncoder>().AsImplementedInterfaces();
                builder.RegisterType<BinarySampleMessageEncoder>().AsImplementedInterfaces();
                builder.RegisterType<JsonSampleMessageEncoder>().AsImplementedInterfaces();
                builder.RegisterType<VariantEncoderFactory>().AsImplementedInterfaces();
                builder.RegisterType<DefaultSessionManager>().AsImplementedInterfaces().SingleInstance();
                builder.RegisterType<SubscriptionServices>().AsImplementedInterfaces().SingleInstance();
                builder.RegisterType<ObservableEventFixture>().AsSelf().AsImplementedInterfaces().SingleInstance();
            });
            return mock;
        }

        private readonly TestServerFixture _server;
        private readonly IPHostEntry _hostEntry;
    }

}
