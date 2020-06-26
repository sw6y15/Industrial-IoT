// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Registry.Services {
    using Microsoft.Azure.IIoT.OpcUa.Registry;
    using Microsoft.Azure.IIoT.OpcUa.Edge;
    using Microsoft.Azure.IIoT.Hub;
    using Microsoft.Azure.IIoT.Utils;
    using Microsoft.Azure.IIoT.Serializers;
    using Serilog;
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Update settings on all module entities
    /// </summary>
    public class SettingsSyncHost : IHostProcess, IDisposable {

        /// <summary>
        /// Create host
        /// </summary>
        /// <param name="twins"></param>
        /// <param name="endpoint"></param>
        /// <param name="config"></param>
        /// <param name="serializer"></param>
        /// <param name="logger"></param>
        public SettingsSyncHost(IIoTHubTwinServices twins, IServiceEndpoint endpoint,
            IJsonSerializer serializer, ILogger logger, ISettingsSyncConfig config = null) {
            _endpoint = endpoint ?? throw new ArgumentNullException(nameof(endpoint));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _twins = twins ?? throw new ArgumentNullException(nameof(twins));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _config = config;
            _updateTimer = new Timer(OnUpdateTimerFiredAsync);
            _endpoint.OnServiceEndpointUpdated += OnServiceEndpointUpdated;
        }

        /// <inheritdoc/>
        public void Dispose() {
            _endpoint.OnServiceEndpointUpdated -= OnServiceEndpointUpdated;
            Try.Async(StopAsync).Wait();
            _updateTimer.Dispose();
        }

        /// <inheritdoc/>
        public Task StartAsync() {
            if (_cts == null) {
                _cts = new CancellationTokenSource();
                _updateTimer.Change(0, Timeout.Infinite);
            }
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public Task StopAsync() {
            if (_cts != null) {
                _cts.Cancel();
                _updateTimer.Change(Timeout.Infinite, Timeout.Infinite);
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Service endpoint was updated - sync now
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void OnServiceEndpointUpdated(object sender, EventArgs e) {
            if (_cts != null) {
                _updateTimer.Change(0, Timeout.Infinite);
            }
        }

        /// <summary>
        /// Timer operation
        /// </summary>
        /// <param name="sender"></param>
        private async void OnUpdateTimerFiredAsync(object sender) {
            try {
                _cts.Token.ThrowIfCancellationRequested();
                _logger.Information("Updating service endpoint urls...");
                await UpdateServiceEndpointsAsync(_cts.Token);
                _logger.Information("Service endpoint Url update finished.");
            }
            catch (OperationCanceledException) {
                // Cancel was called - dispose cancellation token
                _cts.Dispose();
                _cts = null;
                return;
            }
            catch (Exception ex) {
                _logger.Error(ex, "Failed to update service endpoint urls.");
            }
            _updateTimer.Change(_config?.SettingSyncInterval ?? kDefaultInterval,
                Timeout.InfiniteTimeSpan);
        }

        /// <summary>
        /// Update all identity tokens
        /// </summary>
        /// <returns></returns>
        public async Task UpdateServiceEndpointsAsync(CancellationToken ct) {
            var url = _endpoint.ServiceEndpoint?.TrimEnd('/');
            if (string.IsNullOrEmpty(url)) {
                return;
            }
            var query = "SELECT * FROM devices.modules WHERE " +
                $"IS_DEFINED(properties.reported.{TwinProperty.ServiceEndpoint}) AND " +
                $"(NOT IS_DEFINED(properties.desired.{TwinProperty.ServiceEndpoint}) OR " +
                    $"properties.desired.{TwinProperty.ServiceEndpoint} != '{url}')";
            string continuation = null;
            do {
                var response = await _twins.QueryDeviceTwinsAsync(
                    query, continuation, null, ct);
                foreach (var moduleTwin in response.Items) {
                    try {
                        moduleTwin.Properties.Desired[TwinProperty.ServiceEndpoint] =
                            _serializer.FromObject(url);
                        await _twins.PatchAsync(moduleTwin, false, ct);
                    }
                    catch (Exception ex) {
                        _logger.Error(ex, "Failed to update url for module {device} {module}",
                            moduleTwin.Id, moduleTwin.ModuleId);
                    }
                }
                continuation = response.ContinuationToken;
                ct.ThrowIfCancellationRequested();
            }
            while (continuation != null);
            _logger.Information("Job orchestrator url updated to {url} on all twins.", url);
        }

        private static readonly TimeSpan kDefaultInterval = TimeSpan.FromMinutes(1);
        private readonly IServiceEndpoint _endpoint;
        private readonly IJsonSerializer _serializer;
        private readonly IIoTHubTwinServices _twins;
        private readonly ISettingsSyncConfig _config;
        private readonly ILogger _logger;
        private readonly Timer _updateTimer;
#pragma warning disable IDE0069 // Disposable fields should be disposed
        private CancellationTokenSource _cts;
#pragma warning restore IDE0069 // Disposable fields should be disposed
    }
}