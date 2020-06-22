// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Modules.OpcUa.Publisher {
    using Microsoft.Azure.IIoT.Modules.OpcUa.Publisher.Runtime;
    using Microsoft.Azure.IIoT.Modules.OpcUa.Publisher.Controllers;
    using Microsoft.Azure.IIoT.Module.Framework;
    using Microsoft.Azure.IIoT.Module.Framework.Client;
    using Microsoft.Azure.IIoT.Module.Framework.Services;
    using Microsoft.Azure.IIoT.Module;
    using Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Clients;
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Clients;
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Services;
    using Microsoft.Azure.IIoT.OpcUa.Edge.Supervisor.Services;
    using Microsoft.Azure.IIoT.OpcUa.Edge;
    using Microsoft.Azure.IIoT.OpcUa.Protocol.Services;
    using Microsoft.Azure.IIoT.OpcUa.Protocol;
    using Microsoft.Azure.IIoT.Tasks.Default;
    using Microsoft.Azure.IIoT.Hub;
    using Microsoft.Azure.IIoT.Utils;
    using Microsoft.Azure.IIoT.Serializers;
    using Microsoft.Extensions.Configuration;
    using Autofac;
    using Serilog;
    using System;
    using System.Diagnostics;
    using System.Runtime.Loader;
    using System.Threading;
    using System.Threading.Tasks;
    using Prometheus;

    /// <summary>
    /// Module Process
    /// </summary>
    public class ModuleProcess : IProcessControl {

        /// <summary>
        /// Whethr the module is running
        /// </summary>

        public event EventHandler<bool> OnRunning;

        /// <summary>
        /// Create process
        /// </summary>
        /// <param name="config"></param>
        /// <param name="injector"></param>
        public ModuleProcess(IConfiguration config, IInjector injector = null) {
            _config = config;
            _injector = injector;
            _exitCode = 0;
            _exit = new TaskCompletionSource<bool>();
            AssemblyLoadContext.Default.Unloading += _ => _exit.TrySetResult(true);
        }

        /// <inheritdoc/>
        public void Reset() {
            _reset.TrySetResult(true);
        }

        /// <inheritdoc/>
        public void Exit(int exitCode) {

            // Shut down gracefully.
            _exitCode = exitCode;
            _exit.TrySetResult(true);

            if (Host.IsContainer) {
                // Set timer to kill the entire process after 5 minutes.
#pragma warning disable IDE0067 // Dispose objects before losing scope
                var _ = new Timer(o => {
                    Log.Logger.Fatal("Killing non responsive module process!");
                    Process.GetCurrentProcess().Kill();
                }, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
#pragma warning restore IDE0067 // Dispose objects before losing scope
            }
        }

        /// <summary>
        /// Run module host
        /// </summary>
        public async Task<int> RunAsync() {
            // Wait until the module unloads
            while (true) {
                using (var hostScope = ConfigureContainer(_config)) {
                    _reset = new TaskCompletionSource<bool>();
                    var module = hostScope.Resolve<IModuleHost>();
                    var identity = hostScope.Resolve<IIdentity>();
                    ISessionManager sessionManager = null;
                    var logger = hostScope.Resolve<ILogger>();
                    var config = new Config(_config);
                    IMetricServer server = null;
                    try {
                        var version = GetType().Assembly.GetReleaseVersion().ToString();
                        logger.Information("Starting module OpcPublisher version {version}.",
                            version);
                        await module.StartAsync(IdentityType.Publisher,
                            "OpcPublisher", version, this);
                        if (hostScope.TryResolve(out server)) {
                            server.Start();
                        }
                        kPublisherModuleStart.WithLabels(
                            identity.DeviceId ?? "", identity.ModuleId ?? "").Inc();
                        sessionManager = hostScope.Resolve<ISessionManager>();
                        OnRunning?.Invoke(this, true);
                        await Task.WhenAny(_reset.Task, _exit.Task);
                        if (_exit.Task.IsCompleted) {
                            logger.Information("Module exits...");
                            return _exitCode;
                        }
                        _reset = new TaskCompletionSource<bool>();
                        logger.Information("Module reset...");
                    }
                    catch (Exception ex) {
                        logger.Error(ex, "Error during module execution - restarting!");
                    }
                    finally {
                        kPublisherModuleStart.WithLabels(
                            identity.DeviceId ?? "", identity.ModuleId ?? "").Set(0);
                        if (server != null) {
                            await server.StopAsync();
                        }
                        await sessionManager?.StopAsync();
                        await module.StopAsync();
                        OnRunning?.Invoke(this, false);
                        kPublisherModuleStart.WithLabels(
                            identity.DeviceId ?? "", identity.ModuleId ?? "").Set(0);
                    }
                }
            }
        }

        /// <summary>
        /// Autofac configuration.
        /// </summary>
        /// <param name="configuration"></param>
        /// <returns></returns>
        private IContainer ConfigureContainer(IConfiguration configuration) {

            var config = new Config(configuration);
            var builder = new ContainerBuilder();
            var legacyCliOptions = new LegacyCliOptions(configuration);

            // Register configuration interfaces
            builder.RegisterInstance(config)
                .AsImplementedInterfaces();
            builder.RegisterInstance(config.Configuration)
                .AsImplementedInterfaces();
            builder.RegisterInstance(this)
                .AsImplementedInterfaces();

            // Register module and publisher framework ...
            builder.RegisterModule<ModuleFramework>();
            builder.RegisterModule<NewtonSoftJsonModule>();

            if (legacyCliOptions.RunInLegacyMode) {
                // Standalone mode with legacy configuration options.

                builder.AddDiagnostics(config,
                    legacyCliOptions.ToLoggerConfiguration());
                builder.RegisterInstance(legacyCliOptions)
                    .AsImplementedInterfaces();

                // Configure the processing engine from nodes file
                builder.RegisterType<PublishedNodesFileLoader>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();
                // ... and start it
                builder.RegisterType<HostAutoStart>()
                    .AutoActivate()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();

                // Configure root scope as supervisor
                WriterGroupContainerFactory.ConfigureServices(builder);

                builder.RegisterType<WriterGroupStateLogger>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();
                builder.RegisterType<TaskProcessor>()
                    .AsImplementedInterfaces();
            }
            else {
                builder.AddDiagnostics(config);

                // Register supervisor services to manage writer group twins
                builder.RegisterType<SupervisorServices>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();
                builder.RegisterType<WriterGroupContainerFactory>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();

                // Add controllers
                builder.RegisterType<SupervisorMethodsController>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();
                builder.RegisterType<SupervisorSettingsController>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();
            }

            builder.RegisterType<PublisherSettingsController>()
                .AsImplementedInterfaces().InstancePerLifetimeScope();
            builder.RegisterType<DefaultSessionManager>()
                .AsImplementedInterfaces().InstancePerLifetimeScope();

            if (_injector != null) {
                // Inject additional services
                builder.RegisterInstance(_injector)
                    .AsImplementedInterfaces()
                    .ExternallyOwned();

                _injector.Inject(builder);
            }

            return builder.Build();
        }

        /// <summary>
        /// Container factory for writer group twins
        /// </summary>
        public class WriterGroupContainerFactory : IContainerFactory {

            /// <summary>
            /// Create twin container factory
            /// </summary>
            /// <param name="sessions"></param>
            /// <param name="service"></param>
            /// <param name="logger"></param>
            /// <param name="injector"></param>
            public WriterGroupContainerFactory(ISessionManager sessions,
                IServiceEndpoint service, ILogger logger, IInjector injector = null) {
                _sessions = sessions ?? throw new ArgumentNullException(nameof(sessions));
                _service = service ?? throw new ArgumentNullException(nameof(service));
                _logger = logger ?? throw new ArgumentNullException(nameof(logger));
                _injector = injector;
            }

            /// <inheritdoc/>
            public IContainer Create(Action<ContainerBuilder> configure) {

                // Create container for all twin level scopes...
                var builder = new ContainerBuilder();

                // Register supervisor services
                builder.RegisterInstance(_logger)
                    .ExternallyOwned()
                    .AsImplementedInterfaces();
                builder.RegisterInstance(_sessions)
                    .ExternallyOwned()
                    .AsImplementedInterfaces();
                builder.RegisterInstance(_service)
                    .ExternallyOwned()
                    .As<IServiceEndpoint>();

                // Register module framework
                builder.RegisterModule<ModuleFramework>();
                builder.RegisterModule<NewtonSoftJsonModule>();

                // Register writer group controllers
                builder.RegisterType<WriterGroupMethodsController>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();
                builder.RegisterType<WriterGroupSettingsController>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();
                builder.RegisterType<DataSetWriterSettingsController>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();

                ConfigureServices(builder);

                // Load writers from service endpoint and configure the engine
                builder.RegisterType<DataSetWriterRegistryLoader>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();
                builder.RegisterType<PublisherEdgeApiClient>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();
                // Publish writer group state updates back to iot hub
                builder.RegisterType<WriterGroupStatePublisher>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();
                builder.RegisterType<TaskProcessor>()
                    .AsImplementedInterfaces();

                configure?.Invoke(builder);
                _injector?.Inject(builder);

                // Build twin container
                return builder.Build();
            }

            /// <summary>
            /// Configure common publisher container services
            /// </summary>
            /// <param name="builder"></param>
            public static void ConfigureServices(ContainerBuilder builder) {

                // Publisher engine and encoders
                builder.RegisterType<WriterGroupProcessingEngine>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();

                builder.RegisterType<UadpNetworkMessageEncoder>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();
                builder.RegisterType<JsonNetworkMessageEncoder>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();
                builder.RegisterType<BinarySampleMessageEncoder>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();
                builder.RegisterType<JsonSampleMessageEncoder>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();

                // Register dependent opc ua services
                builder.RegisterType<VariantEncoderFactory>()
                    .AsImplementedInterfaces();
                builder.RegisterType<SubscriptionServices>()
                    .AsImplementedInterfaces().InstancePerLifetimeScope();
            }

            private readonly ISessionManager _sessions;
            private readonly IServiceEndpoint _service;
            private readonly IInjector _injector;
            private readonly ILogger _logger;
        }

        private readonly IConfiguration _config;
        private readonly IInjector _injector;
        private readonly TaskCompletionSource<bool> _exit;
        private TaskCompletionSource<bool> _reset;
        private int _exitCode;
        private static readonly Gauge kPublisherModuleStart = Metrics
            .CreateGauge("iiot_edge_publisher_module_start", "publisher module started",
                new GaugeConfiguration {
                    LabelNames = new[] { "deviceid", "module" }
                });
    }
}