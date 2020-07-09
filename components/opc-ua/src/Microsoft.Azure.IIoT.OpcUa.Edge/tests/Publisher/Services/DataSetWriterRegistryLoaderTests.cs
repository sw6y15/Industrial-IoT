// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Services {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Clients;
    using Microsoft.Azure.IIoT.Serializers;
    using Microsoft.Azure.IIoT.Serializers.NewtonSoft;
    using Autofac;
    using Autofac.Extras.Moq;
    using Xunit;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Threading;

    /// <summary>
    /// Test
    /// </summary>
    public class DataSetWriterRegistryLoaderTests {

        [Fact]
        public void DownloadAndAddThenRemoveTest1() {

            var dataSetWriterId = "testid";
            using (var mock = Setup()) {
                // Setup
                var client = mock.Container.Resolve<MockClient>();
                client.Writers.Add(new DataSetWriterModel {
                    DataSetWriterId = dataSetWriterId
                });

                var engine = mock.Container.Resolve<MockEngine>();
                var service = mock.Create<IDataSetWriterRegistryLoader>();

                Assert.Empty(engine.Writers);
                Assert.Empty(service.LoadState);

                // Act
                service.OnDataSetWriterChanged(dataSetWriterId);
                // Wait until loaded
                engine.Changed.WaitOne(kTimeout);

                // Assert
                Assert.Single(engine.Writers);
                Assert.Single(service.LoadState);
                Assert.Equal(dataSetWriterId, engine.Writers.Single().DataSetWriterId);
                Assert.NotNull(service.LoadState[dataSetWriterId]);

                // Act
                service.OnDataSetWriterRemoved(dataSetWriterId);
                // Wait until removed
                engine.Changed.WaitOne(kTimeout);

                // Assert
                Assert.Empty(engine.Writers);
                Assert.Empty(service.LoadState);
            }
        }

        [Fact]
        public void DownloadAndAddThenRemoveTest2() {

            var dataSetWriterId = "testid";
            using (var mock = Setup()) {

                // Setup
                var client = mock.Container.Resolve<MockClient>();
                client.Writers.Add(new DataSetWriterModel {
                    DataSetWriterId = dataSetWriterId
                });

                var engine = mock.Container.Resolve<MockEngine>();
                var service = mock.Create<IDataSetWriterRegistryLoader>();

                Assert.Empty(engine.Writers);
                Assert.Empty(service.LoadState);

                // Act
                service.OnDataSetWriterChanged(dataSetWriterId);
                // Wait until loaded
                engine.Changed.WaitOne(kTimeout);

                // Assert
                Assert.Single(engine.Writers);
                Assert.Single(service.LoadState);
                Assert.Equal(dataSetWriterId, engine.Writers.Single().DataSetWriterId);
                Assert.NotNull(service.LoadState[dataSetWriterId]);

                service.OnDataSetWriterChanged(dataSetWriterId);
                // Wait until loaded
                engine.Changed.WaitOne(kTimeout);

                Assert.Single(engine.Writers);
                Assert.Single(service.LoadState);
                Assert.Equal(dataSetWriterId, engine.Writers.Single().DataSetWriterId);
                Assert.NotNull(service.LoadState[dataSetWriterId]);

                service.OnDataSetWriterRemoved(dataSetWriterId);
                // Wait until removed
                engine.Changed.WaitOne(kTimeout);

                Assert.Empty(engine.Writers);
                Assert.Empty(service.LoadState);
            }
        }


        [Fact]
        public void DownloadAndAddTwoWriters() {

            var dataSetWriter1Id = "testid1";
            var dataSetWriter2Id = "testid2";
            using (var mock = Setup()) {

                // Setup
                var client = mock.Container.Resolve<MockClient>();
                client.Writers.Add(new DataSetWriterModel {
                    DataSetWriterId = dataSetWriter1Id
                });
                client.Writers.Add(new DataSetWriterModel {
                    DataSetWriterId = dataSetWriter2Id
                });

                var engine = mock.Container.Resolve<MockEngine>();
                var service = mock.Create<IDataSetWriterRegistryLoader>();

                Assert.Empty(engine.Writers);
                Assert.Empty(service.LoadState);

                // Act
                service.OnDataSetWriterChanged(dataSetWriter1Id);
                service.OnDataSetWriterChanged(dataSetWriter2Id);
                // Wait until loaded
                engine.Changed.WaitOne(kTimeout);
                if (service.LoadState.Count != 2) {
                    engine.Changed.WaitOne(kTimeout);
                }

                // Assert
                Assert.Equal(2, engine.Writers.Count);
                Assert.Equal(2, service.LoadState.Count);
                Assert.Contains(engine.Writers, w => w.DataSetWriterId == dataSetWriter1Id);
                Assert.Contains(engine.Writers, w => w.DataSetWriterId == dataSetWriter2Id);
                Assert.NotNull(service.LoadState[dataSetWriter1Id]);
                Assert.NotNull(service.LoadState[dataSetWriter2Id]);

                service.OnDataSetWriterRemoved(dataSetWriter1Id);
                service.OnDataSetWriterRemoved(dataSetWriter2Id);
                // Wait until loaded
                engine.Changed.WaitOne(kTimeout);
                if (service.LoadState.Count != 0) {
                    engine.Changed.WaitOne(kTimeout);
                }

                Assert.Empty(engine.Writers);
                Assert.Empty(service.LoadState);
            }
        }

        [Fact]
        public void DownloadOnlyWithServiceEndpoint() {

            var dataSetWriterId = "testid";
            using (var mock = Setup()) {

                // Setup
                var client = mock.Container.Resolve<MockClient>();
                client.Writers.Add(new DataSetWriterModel {
                    DataSetWriterId = dataSetWriterId
                });

                var endpoint = mock.Container.Resolve<MockEndpoint>();
                endpoint.ServiceEndpoint = null;

                var engine = mock.Container.Resolve<MockEngine>();
                var service = mock.Create<IDataSetWriterRegistryLoader>();

                Assert.Empty(engine.Writers);
                Assert.Empty(service.LoadState);

                // Act
                service.OnDataSetWriterChanged(dataSetWriterId);
                service.OnDataSetWriterRemoved(dataSetWriterId);
                service.OnDataSetWriterChanged(dataSetWriterId);
                service.OnDataSetWriterRemoved(dataSetWriterId);
                engine.Changed.WaitOne(1000);
                Assert.Empty(engine.Writers);
                Assert.Empty(service.LoadState);

                endpoint.ServiceEndpoint = "newendpoint";
                engine.Changed.WaitOne(kTimeout);

                // Assert
                Assert.Empty(engine.Writers);
                Assert.Empty(service.LoadState);
            }
        }

        [Fact]
        public void AddRemoveCancelOutWithServiceEndpoint() {

            var dataSetWriter1Id = "testid1";
            var dataSetWriter2Id = "testid2";
            using (var mock = Setup()) {

                // Setup
                var client = mock.Container.Resolve<MockClient>();
                client.Writers.Add(new DataSetWriterModel {
                    DataSetWriterId = dataSetWriter1Id
                });
                client.Writers.Add(new DataSetWriterModel {
                    DataSetWriterId = dataSetWriter2Id
                });

                var endpoint = mock.Container.Resolve<MockEndpoint>();
                endpoint.ServiceEndpoint = null;

                var engine = mock.Container.Resolve<MockEngine>();
                var service = mock.Create<IDataSetWriterRegistryLoader>();

                Assert.Empty(engine.Writers);
                Assert.Empty(service.LoadState);

                // Act
                service.OnDataSetWriterChanged(dataSetWriter1Id);
                service.OnDataSetWriterChanged(dataSetWriter2Id);
                service.OnDataSetWriterChanged(dataSetWriter1Id);
                service.OnDataSetWriterChanged(dataSetWriter2Id);
                service.OnDataSetWriterChanged(dataSetWriter2Id);
                engine.Changed.WaitOne(1000);
                Assert.Empty(engine.Writers);
                Assert.Empty(service.LoadState);

                endpoint.ServiceEndpoint = "newendpoint";
                engine.Changed.WaitOne(kTimeout);

                // Assert
                Assert.Equal(2, engine.Writers.Count);
                Assert.Equal(2, service.LoadState.Count);
                Assert.Contains(engine.Writers, w => w.DataSetWriterId == dataSetWriter1Id);
                Assert.Contains(engine.Writers, w => w.DataSetWriterId == dataSetWriter2Id);
                Assert.NotNull(service.LoadState[dataSetWriter1Id]);
                Assert.NotNull(service.LoadState[dataSetWriter2Id]);
            }
        }

        /// <summary>
        /// Setup mock
        /// </summary>
        /// <param name="mock"></param>
        /// <param name="provider"></param>
        private static AutoMock Setup() {
            var mock = AutoMock.GetLoose(builder => {

                builder.RegisterType<NewtonSoftJsonConverters>().As<IJsonSerializerConverterProvider>();
                builder.RegisterType<NewtonSoftJsonSerializer>().As<IJsonSerializer>();
                builder.RegisterType<MockEndpoint>().AsSelf().As<IServiceEndpoint>().SingleInstance();
                builder.RegisterType<MockEngine>().AsSelf().As<IWriterGroupProcessingEngine>().SingleInstance();
                builder.RegisterType<MockClient>().AsSelf().As<IDataSetWriterRegistryEdgeClient>().SingleInstance();
                builder.RegisterType<DataSetWriterRegistryLoader>().As<IDataSetWriterRegistryLoader>();
            });
            return mock;
        }

        private const int kTimeout = 120000;

        /// <summary>
        /// Mock
        /// </summary>
        public class MockEndpoint : IServiceEndpoint {
            private string _serviceEndpoint = "Testendpoint";

            public string ServiceEndpoint {
                get => _serviceEndpoint;
                set {
                    _serviceEndpoint = value;
                    OnServiceEndpointUpdated?.Invoke(this, null);
                }
            }
            public event EventHandler OnServiceEndpointUpdated;
        }

        /// <summary>
        /// Mock
        /// </summary>
        public class MockClient : IDataSetWriterRegistryEdgeClient {

            public Task<DataSetWriterModel> GetDataSetWriterAsync(string serviceUrl,
                string dataSetWriterId, CancellationToken ct) {
                if (string.IsNullOrEmpty(serviceUrl)) {
                    return null;
                }
                return Task.FromResult(Writers.SingleOrDefault(w => w.DataSetWriterId == dataSetWriterId));
            }

            public List<DataSetWriterModel> Writers { get; } = new List<DataSetWriterModel>();
        }

        public class MockEngine : IWriterGroupProcessingEngine {
            public string MessageSchema { get; set; }
            public string WriterGroupId { get; set; }
            public uint? GroupVersion { get; set; }
            public NetworkMessageContentMask? NetworkMessageContentMask { get; set; }
            public uint? MaxNetworkMessageSize { get; set; }
            public string HeaderLayoutUri { get; set; }
            public int? BatchSize { get; set; }
            public TimeSpan? PublishingInterval { get; set; }
            public TimeSpan? KeepAliveTime { get; set; }
            public DataSetOrderingType? DataSetOrdering { get; set; }
            public double? SamplingOffset { get; set; }
            public List<double> PublishingOffset { get; set; }
            public byte? Priority { get; set; }
            public TimeSpan? DiagnosticsInterval { get; set; }
            public HashSet<DataSetWriterModel> Writers { get; } = new HashSet<DataSetWriterModel>();
            public AutoResetEvent Changed { get; } = new AutoResetEvent(false);

            public void AddWriters(IEnumerable<DataSetWriterModel> dataSetWriters) {
                foreach (var writer in dataSetWriters) {
                    Writers.Add(writer);
                }
                Changed.Set();
            }

            public void RemoveAllWriters() {
                Writers.Clear();
                Changed.Set();
            }

            public void RemoveWriters(IEnumerable<string> dataSetWriters) {
                Writers.RemoveWhere(d => dataSetWriters.Any(w => d.DataSetWriterId == w));
                Changed.Set();
            }
        }
    }
}
