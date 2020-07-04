// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Registry.Services {
    using Microsoft.Azure.IIoT.OpcUa.Registry.Models;
    using Microsoft.Azure.IIoT.Exceptions;
    using Microsoft.Azure.IIoT.Hub;
    using Microsoft.Azure.IIoT.Hub.Mock;
    using Microsoft.Azure.IIoT.Hub.Models;
    using Microsoft.Azure.IIoT.Serializers.NewtonSoft;
    using Microsoft.Azure.IIoT.Serializers;
    using Autofac.Extras.Moq;
    using AutoFixture;
    using AutoFixture.Kernel;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Xunit;
    using Autofac;
    using Microsoft.Azure.IIoT.OpcUa.Publisher;
    using System.Threading.Tasks;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;

    public class PublisherRegistryTests {

        [Fact]
        public void GetPublisherThatDoesNotExist() {
            CreatePublisherFixtures(out var site, out var publishers, out var modules);

            using (var mock = AutoMock.GetLoose(builder => {
                var hub = IoTHubServices.Create(modules);
                builder.RegisterType<NewtonSoftJsonConverters>().As<IJsonSerializerConverterProvider>();
                builder.RegisterType<NewtonSoftJsonSerializer>().As<IJsonSerializer>();
                builder.RegisterInstance(hub).As<IIoTHubTwinServices>();
            })) {
                IPublisherRegistry service = mock.Create<PublisherRegistry>();

                // Run
                var t = service.GetPublisherAsync("test", false);

                // Assert
                Assert.NotNull(t.Exception);
                Assert.IsType<AggregateException>(t.Exception);
                Assert.IsType<ResourceNotFoundException>(t.Exception.InnerException);
            }
        }

        [Fact]
        public void GetPublisherThatExists() {
            CreatePublisherFixtures(out var site, out var publishers, out var modules);

            using (var mock = AutoMock.GetLoose(builder => {
                var hub = IoTHubServices.Create(modules);
                builder.RegisterType<NewtonSoftJsonConverters>().As<IJsonSerializerConverterProvider>();
                builder.RegisterType<NewtonSoftJsonSerializer>().As<IJsonSerializer>();
                builder.RegisterInstance(hub).As<IIoTHubTwinServices>();
            })) {
                IPublisherRegistry service = mock.Create<PublisherRegistry>();

                // Run
                var result = service.GetPublisherAsync(publishers.First().Id, false).Result;

                // Assert
                Assert.True(result.IsSameAs(publishers.First()));
            }
        }

        [Fact]
        public void UpdatePublisherThatExists() {
            CreatePublisherFixtures(out var site, out var publishers, out var modules);

            using (var mock = AutoMock.GetLoose(builder => {
                var hub = IoTHubServices.Create(modules);
                builder.RegisterType<NewtonSoftJsonConverters>().As<IJsonSerializerConverterProvider>();
                builder.RegisterType<NewtonSoftJsonSerializer>().As<IJsonSerializer>();
                builder.RegisterInstance(hub).As<IIoTHubTwinServices>();
            })) {
                IPublisherRegistry service = mock.Create<PublisherRegistry>();

                // Run
                service.UpdatePublisherAsync(publishers.First().Id, new PublisherUpdateModel {
                    LogLevel = TraceLogLevel.Debug
                }).Wait();
                var result = service.GetPublisherAsync(publishers.First().Id, false).Result;


                // Assert
                Assert.Equal(TraceLogLevel.Debug, result.LogLevel);
            }
        }

        [Fact]
        public void ListAllPublishers() {
            CreatePublisherFixtures(out var site, out var publishers, out var modules);

            using (var mock = AutoMock.GetLoose(builder => {
                var hub = IoTHubServices.Create(modules);
                builder.RegisterType<NewtonSoftJsonConverters>().As<IJsonSerializerConverterProvider>();
                builder.RegisterType<NewtonSoftJsonSerializer>().As<IJsonSerializer>();
                builder.RegisterInstance(hub).As<IIoTHubTwinServices>();
            })) {
                IPublisherRegistry service = mock.Create<PublisherRegistry>();

                // Run
                var records = service.ListPublishersAsync(null, false, null).Result;

                // Assert
                Assert.True(publishers.IsSameAs(records.Items));
            }
        }

        [Fact]
        public void ListAllPublishersUsingQuery() {
            CreatePublisherFixtures(out var site, out var publishers, out var modules);

            using (var mock = AutoMock.GetLoose(builder => {
                var hub = IoTHubServices.Create(modules);
                builder.RegisterType<NewtonSoftJsonConverters>().As<IJsonSerializerConverterProvider>();
                builder.RegisterType<NewtonSoftJsonSerializer>().As<IJsonSerializer>();
                builder.RegisterInstance(hub).As<IIoTHubTwinServices>();
            })) {
                IPublisherRegistry service = mock.Create<PublisherRegistry>();

                // Run
                var records = service.QueryPublishersAsync(null, false, null).Result;

                // Assert
                Assert.True(publishers.IsSameAs(records.Items));
            }
        }

        [Fact]
        public async Task WriterGroupEventsTestsAsync() {
            CreatePublisherFixtures(out var site, out var publishers, out var modules);

            using (var mock = AutoMock.GetLoose(builder => {
                var hub = IoTHubServices.Create(modules);
                builder.RegisterType<NewtonSoftJsonConverters>().As<IJsonSerializerConverterProvider>();
                builder.RegisterType<NewtonSoftJsonSerializer>().As<IJsonSerializer>();
                builder.RegisterInstance(hub).As<IIoTHubTwinServices>();
            })) {
                IWriterGroupRegistryListener events = mock.Create<PublisherRegistry>();
                IWriterGroupStatus service = mock.Create<PublisherRegistry>();

                var results = await service.ListAllWriterGroupActivationsAsync();
                Assert.Empty(results);

                // Run
                await events.OnWriterGroupAddedAsync(null, new WriterGroupInfoModel {
                    WriterGroupId = "testid",
                    BatchSize = 5,
                    LocaleIds = new List<string> { "test" }
                });

                // Assert
                results = await service.ListAllWriterGroupActivationsAsync();
                Assert.Empty(results);
                results = await service.ListAllWriterGroupActivationsAsync(false);
                Assert.Empty(results);

                // Run
                await events.OnWriterGroupActivatedAsync(null, new WriterGroupInfoModel {
                    WriterGroupId = "testid"
                });

                // Assert
                results = await service.ListAllWriterGroupActivationsAsync();
                Assert.Empty(results); // Not placed and connected
                results = await service.ListAllWriterGroupActivationsAsync(false);
                Assert.Single(results);

                // Run
                await events.OnWriterGroupDeactivatedAsync(null, new WriterGroupInfoModel {
                    WriterGroupId = "testid"
                });

                // Assert
                results = await service.ListAllWriterGroupActivationsAsync(false);
                Assert.Empty(results);
                results = await service.ListAllWriterGroupActivationsAsync();
                Assert.Empty(results);

                // Run
                await events.OnWriterGroupActivatedAsync(null, new WriterGroupInfoModel {
                    WriterGroupId = "testid"
                });

                // Assert
                results = await service.ListAllWriterGroupActivationsAsync();
                Assert.Empty(results); // Not placed and connected
                results = await service.ListAllWriterGroupActivationsAsync(false);
                Assert.Single(results);

                // Run
                await events.OnWriterGroupRemovedAsync(null, "testid");
                // Assert
                results = await service.ListAllWriterGroupActivationsAsync();
                Assert.Empty(results);
                results = await service.ListAllWriterGroupActivationsAsync(false);
                Assert.Empty(results);
            }
        }

        [Fact]
        public async Task DataSetWriterEventsTestsAsync() {
            CreatePublisherFixtures(out var site, out var publishers, out var modules);

            var hub = IoTHubServices.Create(modules);
            using (var mock = AutoMock.GetLoose(builder => {
                builder.RegisterType<NewtonSoftJsonConverters>().As<IJsonSerializerConverterProvider>();
                builder.RegisterType<NewtonSoftJsonSerializer>().As<IJsonSerializer>();
                builder.RegisterInstance(hub).As<IIoTHubTwinServices>();
            })) {
                IWriterGroupRegistryListener events = mock.Create<PublisherRegistry>();
                IDataSetWriterRegistryListener writers = mock.Create<PublisherRegistry>();

                // Run
                var groupid = "testid";
                await events.OnWriterGroupAddedAsync(null, new WriterGroupInfoModel {
                    WriterGroupId = groupid,
                    BatchSize = 5,
                    LocaleIds = new List<string> { "test" }
                });

                await writers.OnDataSetWriterAddedAsync(null, new DataSetWriterInfoModel {
                    DataSetWriterId = "dw1",
                    WriterGroupId = groupid
                });

                var device = await hub.GetAsync(PublisherRegistryEx.ToDeviceId(groupid), null, default);
                Assert.Contains(PublisherRegistryEx.ToPropertyName("dw1"), device.Properties.Desired.Keys);

                await writers.OnDataSetWriterAddedAsync(null, new DataSetWriterInfoModel {
                    DataSetWriterId = "dw2",
                    WriterGroupId = groupid
                });

                device = await hub.GetAsync(PublisherRegistryEx.ToDeviceId(groupid), null, default);
                Assert.Contains(PublisherRegistryEx.ToPropertyName("dw1"), device.Properties.Desired.Keys);
                Assert.Contains(PublisherRegistryEx.ToPropertyName("dw2"), device.Properties.Desired.Keys);

                await writers.OnDataSetWriterRemovedAsync(null, new DataSetWriterInfoModel {
                    DataSetWriterId = "dw1",
                    WriterGroupId = groupid
                });

                device = await hub.GetAsync(PublisherRegistryEx.ToDeviceId(groupid), null, default);
                Assert.DoesNotContain(PublisherRegistryEx.ToPropertyName("dw1"), device.Properties.Desired.Keys);
                Assert.Contains(PublisherRegistryEx.ToPropertyName("dw2"), device.Properties.Desired.Keys);

                await writers.OnDataSetWriterUpdatedAsync(null, "dw2", new DataSetWriterInfoModel {
                    IsDisabled = true
                });

                device = await hub.GetAsync(PublisherRegistryEx.ToDeviceId(groupid), null, default);
                Assert.DoesNotContain(PublisherRegistryEx.ToPropertyName("dw1"), device.Properties.Desired.Keys);
                Assert.DoesNotContain(PublisherRegistryEx.ToPropertyName("dw2"), device.Properties.Desired.Keys);

                await writers.OnDataSetWriterUpdatedAsync(null, "dw2", new DataSetWriterInfoModel {
                    DataSetWriterId = "dw2",
                    WriterGroupId = groupid,
                    IsDisabled = false
                });

                device = await hub.GetAsync(PublisherRegistryEx.ToDeviceId(groupid), null, default);
                Assert.DoesNotContain(PublisherRegistryEx.ToPropertyName("dw1"), device.Properties.Desired.Keys);
                Assert.Contains(PublisherRegistryEx.ToPropertyName("dw2"), device.Properties.Desired.Keys);

                await writers.OnDataSetWriterRemovedAsync(null, new DataSetWriterInfoModel {
                    DataSetWriterId = "dw2",
                    WriterGroupId = groupid
                });

                device = await hub.GetAsync(PublisherRegistryEx.ToDeviceId(groupid), null, default);
                Assert.DoesNotContain(PublisherRegistryEx.ToPropertyName("dw1"), device.Properties.Desired.Keys);
                Assert.DoesNotContain(PublisherRegistryEx.ToPropertyName("dw2"), device.Properties.Desired.Keys);
            }
        }

        /// <summary>
        /// Helper to create app fixtures
        /// </summary>
        /// <param name="site"></param>
        /// <param name="publishers"></param>
        /// <param name="modules"></param>
        private void CreatePublisherFixtures(out string site,
            out List<PublisherModel> publishers, out List<(DeviceTwinModel, DeviceModel)> modules,
            bool noSite = false) {
            var fix = new Fixture();
            fix.Customizations.Add(new TypeRelay(typeof(VariantValue), typeof(VariantValue)));
            fix.Behaviors.OfType<ThrowingRecursionBehavior>().ToList()
                .ForEach(b => fix.Behaviors.Remove(b));
            fix.Behaviors.Add(new OmitOnRecursionBehavior());
            var sitex = site = noSite ? null : fix.Create<string>();
            publishers = fix
                .Build<PublisherModel>()
                .Without(x => x.Id)
                .Do(x => x.Id = PublisherModelEx.CreatePublisherId(
                    fix.Create<string>(), fix.Create<string>()))
                .CreateMany(10)
                .ToList();

            modules = publishers
                .Select(a => a.ToPublisherRegistration())
                .Select(a => a.ToDeviceTwin(_serializer))
                .Select(t => {
                    t.Properties.Reported = new Dictionary<string, VariantValue> {
                        [TwinProperty.Type] = IdentityType.Publisher
                    };
                    return t;
                })
                .Select(t => (t, new DeviceModel { Id = t.Id, ModuleId = t.ModuleId }))
                .ToList();
        }

        private readonly IJsonSerializer _serializer = new NewtonSoftJsonSerializer();
    }
}
