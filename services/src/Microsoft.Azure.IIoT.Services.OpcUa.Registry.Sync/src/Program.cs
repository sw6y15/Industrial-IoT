// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Services.OpcUa.Registry.Sync {
    using Microsoft.Azure.IIoT.Services.OpcUa.Registry.Sync.Runtime;
    using Microsoft.Azure.IIoT.OpcUa.Registry.Handlers;
    using Microsoft.Azure.IIoT.OpcUa.Registry.Services;
    using Microsoft.Azure.IIoT.OpcUa.Registry;
    using Microsoft.Azure.IIoT.OpcUa.Api.Registry.Clients;
    using Microsoft.Azure.IIoT.OpcUa.Api.Twin.Clients;
    using Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Clients;
    using Microsoft.Azure.IIoT.Http.Default;
    using Microsoft.Azure.IIoT.Http.Ssl;
    using Microsoft.Azure.IIoT.Hub.Client;
    using Microsoft.Azure.IIoT.Serializers;
    using Microsoft.Azure.IIoT.Tasks.Default;
    using Microsoft.Azure.IIoT.Auth.Clients;
    using Microsoft.Azure.IIoT.Module.Default;
    using Microsoft.Azure.IIoT.Messaging.Default;
    using Microsoft.Azure.IIoT.Messaging.ServiceBus.Clients;
    using Microsoft.Azure.IIoT.Messaging.ServiceBus.Services;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;
    using Autofac;
    using Autofac.Extensions.DependencyInjection;
    using Serilog;
    using System;

    /// <summary>
    /// Sync service handles jobs out of process for other services.
    /// </summary>
    public class Program {

        /// <summary>
        /// Main entry point for Sync service
        /// </summary>
        /// <param name="args"></param>
        public static void Main(string[] args) {
            CreateHostBuilder(args).Build().Run();
        }

        /// <summary>
        /// Create host builder
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public static IHostBuilder CreateHostBuilder(string[] args) {
            return Host.CreateDefaultBuilder(args)
                .ConfigureHostConfiguration(configHost => {
                    configHost.AddFromDotEnvFile()
                    .AddEnvironmentVariables()
                    .AddEnvironmentVariables(EnvironmentVariableTarget.User)
                    // Above configuration providers will provide connection
                    // details for KeyVault configuration provider.
                    .AddFromKeyVault(providerPriority: ConfigurationProviderPriority.Lowest);
                })
                .UseServiceProviderFactory(new AutofacServiceProviderFactory())
                .ConfigureContainer<ContainerBuilder>((hostBuilderContext, builder) => {
                    // registering services in the Autofac ContainerBuilder
                    ConfigureContainer(builder, hostBuilderContext.Configuration);
                })
                .ConfigureServices((hostBuilderContext, services) => {
                    services.AddHostedService<HostStarterService>();
                })
                .UseSerilog();
        }

        /// <summary>
        /// Autofac configuration.
        /// </summary>
        /// <param name="builder"></param>
        /// <param name="configuration"></param>
        public static ContainerBuilder ConfigureContainer(ContainerBuilder builder,
            IConfiguration configuration) {

            var serviceInfo = new ServiceInfo();
            var config = new Config(configuration);

            builder.RegisterInstance(serviceInfo)
                .AsImplementedInterfaces();

            // Register configuration interfaces
            builder.RegisterInstance(config)
                .AsSelf()
                .AsImplementedInterfaces();
            builder.RegisterInstance(config.Configuration)
                .AsImplementedInterfaces();

            // Add Application Insights dependency tracking.
            builder.AddDependencyTracking(config, serviceInfo);

            // Add diagnostics
            builder.AddDiagnostics(config);

            // Register http client module
            builder.RegisterModule<HttpClientModule>();
#if DEBUG
            builder.RegisterType<NoOpCertValidator>()
                .AsImplementedInterfaces();
#endif
            // Add serializers
            builder.RegisterModule<NewtonSoftJsonModule>();

            // Add unattended authentication
            builder.RegisterModule<UnattendedAuthentication>();

            // Iot hub services
            builder.RegisterType<IoTHubServiceHttpClient>()
                .AsImplementedInterfaces();
            builder.RegisterType<IoTHubTwinMethodClient>()
                .AsImplementedInterfaces();
            builder.RegisterType<ChunkMethodClient>()
                .AsImplementedInterfaces();

            // Register event bus
            builder.RegisterType<EventBusHost>()
                .AsImplementedInterfaces().SingleInstance();
            builder.RegisterType<ServiceBusClientFactory>()
                .AsImplementedInterfaces();
            builder.RegisterType<ServiceBusEventBus>()
                .AsImplementedInterfaces().SingleInstance();

            // Register task processor
            builder.RegisterType<TaskProcessor>()
                .AsImplementedInterfaces().SingleInstance();

            // Handle discovery request and pass to all edges
            builder.RegisterModule<RegistryServices>();
            builder.RegisterType<DiscoveryRequestHandler>()
                .AsImplementedInterfaces();
            builder.RegisterType<DiscovererModuleClient>()
                .AsImplementedInterfaces();
            builder.RegisterType<DiscoveryMultiplexer>()
                .AsImplementedInterfaces().SingleInstance();

            // Supervisor Activation and Settings sync
            builder.RegisterType<TwinModuleActivationClient>()
                .AsImplementedInterfaces();
            builder.RegisterType<TwinModuleCertificateClient>()
                .AsImplementedInterfaces();
            builder.RegisterType<TwinModuleDiagnosticsClient>()
                .AsImplementedInterfaces();
            builder.RegisterType<PublisherModuleActivationClient>()
                .AsImplementedInterfaces();

            builder.RegisterType<ActivationSyncHost>()
                .AsImplementedInterfaces().SingleInstance();
            builder.RegisterType<SettingsSyncHost>()
                .AsImplementedInterfaces().SingleInstance();
            builder.RegisterType<OrchestrationHost>()
                .AsImplementedInterfaces().SingleInstance();

            return builder;
        }
    }
}
