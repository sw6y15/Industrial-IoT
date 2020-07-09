// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Services.OpcUa.Registry.Sync.Runtime {
    using Microsoft.Azure.IIoT.OpcUa.Registry.Runtime;
    using Microsoft.Azure.IIoT.OpcUa.Registry;
    using Microsoft.Azure.IIoT.OpcUa.Edge;
    using Microsoft.Azure.IIoT.Messaging.ServiceBus;
    using Microsoft.Azure.IIoT.Messaging.ServiceBus.Runtime;
    using Microsoft.Azure.IIoT.Hub.Client;
    using Microsoft.Azure.IIoT.Hub.Client.Runtime;
    using Microsoft.Azure.IIoT.AspNetCore.Diagnostics;
    using Microsoft.Azure.IIoT.Auth;
    using Microsoft.Azure.IIoT.Diagnostics;
    using Microsoft.Extensions.Configuration;
    using System;

    /// <summary>
    /// Registry sync configuration
    /// </summary>
    public class Config : DiagnosticsConfig, IIoTHubConfig, IServiceBusConfig,
        IActivationSyncConfig, IServiceEndpoint, IOrchestrationConfig, IMetricServerConfig,
        ISettingsSyncConfig {

        /// <inheritdoc/>
        public string IoTHubConnString => _hub.IoTHubConnString;
        /// <inheritdoc/>
        public string ServiceBusConnString => _sb.ServiceBusConnString;
        /// <inheritdoc/>
        public TimeSpan? ActivationSyncInterval => _sync.ActivationSyncInterval;
        /// <inheritdoc/>
        public TimeSpan? UpdatePlacementInterval => _or.UpdatePlacementInterval;

        /// <inheritdoc/>
        public TimeSpan? SettingSyncInterval => _ep.SettingSyncInterval;
        /// <inheritdoc/>
        public string ServiceEndpoint => _ep.ServiceEndpoint;
        /// <inheritdoc/>
        public event EventHandler OnServiceEndpointUpdated {
            add => _ep.OnServiceEndpointUpdated += value;
            remove => _ep.OnServiceEndpointUpdated -= value;
        }

        /// <inheritdoc/>
        public int Port => 9505;

        /// <summary>
        /// Configuration constructor
        /// </summary>
        /// <param name="configuration"></param>
        public Config(IConfiguration configuration) :
            base(configuration) {

            _sb = new ServiceBusConfig(configuration);
            _hub = new IoTHubConfig(configuration);
            _sync = new ActivationSyncConfig(configuration);
            _ep = new SettingsSyncConfig(configuration);
            _or = new OrchestrationConfig(configuration);
        }

        private readonly IServiceBusConfig _sb;
        private readonly IIoTHubConfig _hub;
        private readonly SettingsSyncConfig _ep;
        private readonly ActivationSyncConfig _sync;
        private readonly OrchestrationConfig _or;
    }
}
