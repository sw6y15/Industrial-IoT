// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Services {
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Clients;
    using Microsoft.Azure.IIoT.Utils;
    using System;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using Serilog;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Linq;

    /// <summary>
    /// Loads writer group writers from the registry endpoint and reconfigures
    /// the writer group processing engine on the fly.
    /// </summary>
    public class DataSetWriterRegistryLoader : IDataSetWriterRegistryLoader,
        IDisposable {

        /// <inheritdoc/>
        public IDictionary<string, string> LoadState => _state;

        /// <summary>
        /// Create connector
        /// </summary>
        /// <param name="client"></param>
        /// <param name="engine"></param>
        /// <param name="endpoint"></param>
        /// <param name="logger"></param>
        public DataSetWriterRegistryLoader(IDataSetWriterRegistryEdgeClient client,
            IWriterGroupProcessingEngine engine, IServiceEndpoint endpoint,
            ILogger logger) {
            _engine = engine ?? throw new ArgumentNullException(nameof(engine));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _endpoint = endpoint ?? throw new ArgumentNullException(nameof(endpoint));
            _trigger = new TaskTrigger(LoadAnyAsync);
            _state = new ConcurrentDictionary<string, string>();
            _writerIds = new ConcurrentDictionary<string, bool>();
            _serviceEndpoint = _endpoint.ServiceEndpoint;
            endpoint.OnServiceEndpointUpdated += OnServiceEndpointUpdated;
        }

        /// <inheritdoc/>
        public void OnDataSetWriterChanged(string dataSetWriterId) {
            _writerIds.AddOrUpdate(dataSetWriterId, false, (k, b) => false);
            _trigger.Pull();
        }

        /// <inheritdoc/>
        public void OnDataSetWriterRemoved(string dataSetWriterId) {
            _writerIds.AddOrUpdate(dataSetWriterId, true, (k, b) => true);
            _trigger.Pull();
        }

        /// <inheritdoc/>
        public void Dispose() {
            _endpoint.OnServiceEndpointUpdated -= OnServiceEndpointUpdated;
            _writerIds.Clear();
            // Schedule cancellation
            _trigger.DisposeAsync().AsTask().Wait();
        }

        /// <summary>
        /// Downloads all writers and updates engine
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        private async Task LoadAnyAsync(CancellationToken ct) {
            var serviceEndpoint = _serviceEndpoint;
            if (string.IsNullOrEmpty(serviceEndpoint)) {
                return;
            }
            // Copy all currently processed keys
            var processing = _writerIds.Keys.ToList();
            if (processing.Count == 0) {
                return;
            }
            var toRemove = new List<string>();
            var toDownload = new List<string>();
            processing.ForEach(writer => {
                // Pull from writer ids and add to either bag
                if (_writerIds.TryRemove(writer, out var remove)) {
                    if (remove) {
                        toRemove.Add(writer);
                        _state.TryRemove(writer, out _);
                    }
                    else {
                        toDownload.Add(writer);
                    }
                }
            });
            _logger.Debug("Applying DataSet changes to engine for group {writerGroup}...",
                 _engine.WriterGroupId);

            // Download all writers needed in parallel
            var downloaded = await Task.WhenAll(toDownload.Select(async writerId => {
                try {
                    _logger.Information("Loading DataSet {writerId} for {writerGroup}.",
                        writerId, _engine.WriterGroupId);
                    var result = await _client.GetDataSetWriterAsync(serviceEndpoint,
                        writerId, ct);
                    _state.AddOrUpdate(writerId, DateTime.UtcNow.ToString());
                    return result;
                }
                catch (Exception ex) {
                    // Re-add if gone, but do not touch last state if it already exists
                    _writerIds.AddOrUpdate(writerId, true, (k, b) => b);
                    _state.AddOrUpdate(writerId, ex.Message);
                    _logger.Error(ex, "Failed to download writer {writerId} for {writerGroup}.",
                        writerId, _engine.WriterGroupId);
                    return null;
                }
            }));

            _logger.Debug("Downloaded all writers for writer group {writerGroup}...",
                 _engine.WriterGroupId);

            if (!ct.IsCancellationRequested) {
                // Change engine - this should never fail
                if (toRemove.Count != 0) {
                    _engine.RemoveWriters(toRemove);
                }
                var toAdd = downloaded.Where(d => d != null);
                if (toAdd.Any()) {
                    _engine.AddWriters(toAdd);
                }
                _logger.Information("Applied changes to writer group {writerGroup}.",
                    _engine.WriterGroupId);
            }
        }

        /// <summary>
        /// Handle service endpoint updates
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void OnServiceEndpointUpdated(object sender, EventArgs e) {
            _serviceEndpoint = _endpoint.ServiceEndpoint;
            if (!string.IsNullOrEmpty(_serviceEndpoint)) {
                _trigger.Pull();
            }
        }

        private readonly IWriterGroupProcessingEngine _engine;
        private readonly IDataSetWriterRegistryEdgeClient _client;
        private readonly ILogger _logger;
        private readonly IServiceEndpoint _endpoint;
        private readonly TaskTrigger _trigger;
        private readonly ConcurrentDictionary<string, bool> _writerIds;
        private readonly ConcurrentDictionary<string, string> _state;
        private string _serviceEndpoint;
    }
}