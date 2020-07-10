// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Services {
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Protocol;
    using Microsoft.Azure.IIoT.OpcUa.Protocol.Models;
    using Microsoft.Azure.IIoT.Utils;
    using Serilog;
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;

    /// <summary>
    /// Collects / receives data for writers in the writer group, contextualizes
    /// them and enqueues them to the writer group message emitter.
    /// </summary>
    public class WriterGroupDataCollector : IWriterGroupDataCollector,
        IDisposable {

        /// <inheritdoc/>
        public uint? GroupVersion { get; set; }

        /// <inheritdoc/>
        public double? SamplingOffset { get; set; }

        /// <inheritdoc/>
        public TimeSpan? KeepAliveTime { get; set; }

        /// <inheritdoc/>
        public byte? Priority { get; set; }

        /// <summary>
        /// Create writer group processor
        /// </summary>
        /// <param name="diagnostics"></param>
        /// <param name="state"></param>
        /// <param name="emitter"></param>
        /// <param name="subscriptions"></param>
        /// <param name="logger"></param>
        public WriterGroupDataCollector(ISubscriptionManager subscriptions,
            IWriterGroupMessageEmitter emitter, IWriterGroupDiagnostics diagnostics,
            IWriterGroupStateReporter state, ILogger logger) {
            _state = state ?? throw new ArgumentNullException(nameof(state));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _subscriptions = subscriptions ?? throw new ArgumentNullException(nameof(subscriptions));
            _emitter = emitter ?? throw new ArgumentNullException(nameof(emitter));
            _diagnostics = diagnostics ?? throw new ArgumentNullException(nameof(diagnostics));
            _writers = new ConcurrentDictionary<string, DataSetWriterSubscription>();
        }

        /// <inheritdoc/>
        public void AddWriters(IEnumerable<DataSetWriterModel> dataSetWriters) {

            // TODO capture tasks

            foreach (var writer in dataSetWriters) {
                _writers.AddOrUpdate(writer.DataSetWriterId, writerId => {
                    var subscription = new DataSetWriterSubscription(this, writer);
                    subscription.OpenAsync().ContinueWith(_ => subscription.ActivateAsync());
                    return subscription;
                }, (writerId, subscription) => {
                    subscription.DeactivateAsync().ContinueWith(_ => subscription.Dispose());
                    subscription = new DataSetWriterSubscription(this, writer);
                    subscription.OpenAsync().ContinueWith(_ => subscription.ActivateAsync());
                    return subscription;
                });
            }
        }

        /// <inheritdoc/>
        public void RemoveWriters(IEnumerable<string> dataSetWriters) {

            // TODO capture tasks

            foreach (var writer in dataSetWriters) {
                if (_writers.TryRemove(writer, out var subscription)) {
                    // TODO: Add cleanup
                    subscription.DeactivateAsync().ContinueWith(_ => subscription.Dispose());
                }
            }
        }

        /// <inheritdoc/>
        public void RemoveAllWriters() {

            // TODO capture tasks
            var writers = _writers.Values.ToList();
            _writers.Clear();
            try {
                Task.WhenAll(writers.Select(sc => sc.DeactivateAsync())).Wait();
            }
            finally {
                writers.ForEach(writer => writer.Dispose());
            }
        }

        /// <inheritdoc/>
        public void Dispose() {
            try {
                // Stop
                RemoveAllWriters();
            }
            catch {
                // Nothing...
            }
            finally {
                _writers.Clear();
            }
        }

        /// <summary>
        /// Handle subscription notification messages
        /// </summary>
        /// <param name="dataSetWriter"></param>
        /// <param name="sequenceNumber"></param>
        /// <param name="notification"></param>
        private void OnSubscriptionNotification(DataSetWriterModel dataSetWriter,
            uint sequenceNumber, SubscriptionNotificationModel notification) {
            try {
                var notifications = notification.Notifications.ToList();
                var message = new DataSetWriterMessageModel {
                    Notifications = notifications,
                    ServiceMessageContext = notification.ServiceMessageContext,
                    SubscriptionId = notification.SubscriptionId,
                    SequenceNumber = sequenceNumber,
                    ApplicationUri = notification.ApplicationUri,
                    EndpointUrl = notification.EndpointUrl,
                    TimeStamp = notification.Timestamp,
                    Writer = dataSetWriter
                };
                _diagnostics.ReportDataSetWriterSubscriptionNotifications(
                    _emitter.WriterGroupId, dataSetWriter.DataSetWriterId, notifications.Count);

                _emitter.Enqueue(message);
            }
            catch (Exception ex) {
                _logger.Debug(ex, "Failed to produce message");
            }
        }

        /// <summary>
        /// A dataset writer
        /// </summary>
        private sealed class DataSetWriterSubscription : IDisposable {

            /// <summary>
            /// Active subscription
            /// </summary>
            public ISubscription Subscription { get; set; }

            /// <summary>
            /// Create subscription from template
            /// </summary>
            /// <param name="outer"></param>
            /// <param name="dataSetWriter"></param>
            public DataSetWriterSubscription(WriterGroupDataCollector outer,
                DataSetWriterModel dataSetWriter) {

                _outer = outer ??
                    throw new ArgumentNullException(nameof(outer));
                _logger = _outer._logger?.ForContext<DataSetWriterSubscription>() ??
                    throw new ArgumentNullException(nameof(_logger));
                _dataSetWriter = dataSetWriter.Clone() ??
                    throw new ArgumentNullException(nameof(dataSetWriter));
                _subscriptionInfo = _dataSetWriter.ToSubscriptionModel();

                if (dataSetWriter.KeyFrameInterval.HasValue &&
                   dataSetWriter.KeyFrameInterval.Value > TimeSpan.Zero) {
                    _keyframeTimer = new System.Timers.Timer(
                        dataSetWriter.KeyFrameInterval.Value.TotalMilliseconds);
                    _keyframeTimer.Elapsed += KeyframeTimerElapsedAsync;
                }
                else {
                    _keyFrameCount = dataSetWriter.KeyFrameCount;
                }

                if (dataSetWriter.DataSetMetaDataSendInterval.HasValue &&
                    dataSetWriter.DataSetMetaDataSendInterval.Value > TimeSpan.Zero) {
                    _metaData = dataSetWriter.DataSet?.DataSetMetaData ??
                        throw new ArgumentNullException(nameof(dataSetWriter.DataSet));

                    _metadataTimer = new System.Timers.Timer(
                        dataSetWriter.DataSetMetaDataSendInterval.Value.TotalMilliseconds);
                    _metadataTimer.Elapsed += MetadataTimerElapsed;
                }
            }

            /// <summary>
            /// Open subscription
            /// </summary>
            /// <returns></returns>
            public async Task OpenAsync() {
                if (Subscription != null) {
                    _logger.Warning("Subscription already exists");
                    return;
                }

                var sc = await _outer._subscriptions.GetOrCreateSubscriptionAsync(_subscriptionInfo);

                sc.OnSubscriptionNotification += OnSubscriptionNotificationAsync;
                sc.OnMonitoredItemStatusChange += OnMonitoredItemStatusChange;
                sc.OnSubscriptionStatusChange += OnSubscriptionStatusChange;
                sc.OnEndpointConnectivityChange += OnEndpointConnectivityChange;

                await sc.ApplyAsync(_subscriptionInfo.MonitoredItems,
                    _subscriptionInfo.Configuration);
                Subscription = sc;
            }

            /// <summary>
            /// activate a subscription
            /// </summary>
            /// <returns></returns>
            public async Task ActivateAsync() {
                if (Subscription == null) {
                    _logger.Warning("Subscription not registered");
                    return;
                }

                // only try to activate if already enabled. Otherwise the activation
                // will be handled by the session's keep alive mechanism
                if (Subscription.Enabled) {
                    await Subscription.ActivateAsync(null).ConfigureAwait(false);
                }

                if (_keyframeTimer != null) {
                    _keyframeTimer.Start();
                }

                if (_metadataTimer != null) {
                    _metadataTimer.Start();
                }
            }

            /// <summary>
            /// deactivate a subscription
            /// </summary>
            /// <returns></returns>
            public async Task DeactivateAsync() {

                if (Subscription == null) {
                    _logger.Warning("Subscription not registered");
                    return;
                }

                await Subscription.CloseAsync().ConfigureAwait(false);

                if (_keyframeTimer != null) {
                    _keyframeTimer.Stop();
                }

                if (_metadataTimer != null) {
                    _metadataTimer.Stop();
                }
            }

            /// <inheritdoc/>
            public void Dispose() {
                if (Subscription != null) {

                    Subscription.OnSubscriptionNotification -= OnSubscriptionNotificationAsync;
                    Subscription.OnMonitoredItemStatusChange -= OnMonitoredItemStatusChange;
                    Subscription.OnSubscriptionStatusChange -= OnSubscriptionStatusChange;
                    Subscription.OnEndpointConnectivityChange -= OnEndpointConnectivityChange;

                    Subscription.Dispose();
                }
                _keyframeTimer?.Dispose();
                _metadataTimer?.Dispose();
                Subscription = null;
            }

            /// <summary>
            /// Fire when keyframe timer elapsed to send keyframe message
            /// </summary>
            /// <param name="sender"></param>
            /// <param name="e"></param>
            private async void KeyframeTimerElapsedAsync(object sender, System.Timers.ElapsedEventArgs e) {
                try {
                    _keyframeTimer.Enabled = false;

                    _logger.Debug("Insert keyframe message...");
                    var sequenceNumber = (uint)Interlocked.Increment(ref _currentSequenceNumber);
                    var snapshot = await Subscription.GetSnapshotAsync();
                    if (snapshot != null) {
                        _outer.OnSubscriptionNotification(_dataSetWriter, sequenceNumber, snapshot);
                    }
                }
                catch (Exception ex) {
                    _logger.Information(ex, "Failed to send keyframe.");
                }
                finally {
                    _keyframeTimer.Enabled = true;
                }
            }

            /// <summary>
            /// Fired when metadata time elapsed
            /// </summary>
            /// <param name="sender"></param>
            /// <param name="e"></param>
            private void MetadataTimerElapsed(object sender, System.Timers.ElapsedEventArgs e) {
                // Send(_metaData)
            }

            /// <summary>
            /// Handle subscription status changes
            /// </summary>
            /// <param name="sender"></param>
            /// <param name="e"></param>
            private void OnSubscriptionStatusChange(object sender, SubscriptionStatusModel e) {
                var state = new PublishedDataSetSourceStateModel {
                    LastResultChange = DateTime.UtcNow,
                    LastResult = e.Error,
                };
                _outer._state.OnDataSetWriterStateChange(_dataSetWriter.DataSetWriterId, state);
            }

            /// <summary>
            /// Handle connectivity status change
            /// </summary>
            /// <param name="sender"></param>
            /// <param name="e"></param>
            private void OnEndpointConnectivityChange(object sender, EndpointConnectivityState e) {
                var state = new PublishedDataSetSourceStateModel {
                    EndpointState = e,
                };
                _outer._state.OnDataSetWriterStateChange(_dataSetWriter.DataSetWriterId, state);
                if (e == EndpointConnectivityState.Connecting) {
                    _outer._diagnostics.ReportConnectionRetry(_outer._emitter.WriterGroupId,
                        _dataSetWriter.DataSetWriterId);
                }
            }

            /// <summary>
            /// Process monitored item status change message
            /// </summary>
            /// <param name="sender"></param>
            /// <param name="e"></param>
            private void OnMonitoredItemStatusChange(object sender, MonitoredItemStatusModel e) {
                var state = new PublishedDataSetItemStateModel {
                    LastResultChange = DateTime.UtcNow,
                    LastResult = e.Error,
                    ServerId = e.ServerId,
                    ClientId = e.ClientHandle,

                    // ...
                };
                if (!e.IsEvent) {
                    // Report as variable state change
                    _outer._state.OnDataSetVariableStateChange(_dataSetWriter.DataSetWriterId,
                        e.Id, state);
                }
                else {
                    // Report as event state
                    _outer._state.OnDataSetEventStateChange(_dataSetWriter.DataSetWriterId, state);
                }
            }

            /// <summary>
            /// Handle subscription change messages
            /// </summary>
            /// <param name="sender"></param>
            /// <param name="e"></param>
            private async void OnSubscriptionNotificationAsync(object sender, SubscriptionNotificationModel e) {
                try {
                    var sequenceNumber = (uint)Interlocked.Increment(ref _currentSequenceNumber);
                    if (_keyFrameCount.HasValue && _keyFrameCount.Value != 0 &&
                        (sequenceNumber % _keyFrameCount.Value) == 0) {
                        var snapshot = await Try.Async(() => Subscription.GetSnapshotAsync());
                        if (snapshot != null) {
                            e = snapshot;
                        }
                    }
                    _outer.OnSubscriptionNotification(_dataSetWriter, sequenceNumber, e);
                }
                catch (Exception ex) {
                    _logger.Error(ex, "Failed to process writer notification");
                }
            }

            private readonly System.Timers.Timer _keyframeTimer;
            private readonly System.Timers.Timer _metadataTimer;
            private readonly uint? _keyFrameCount;
            private long _currentSequenceNumber;
            private readonly WriterGroupDataCollector _outer;
            private readonly DataSetWriterModel _dataSetWriter;
            private readonly DataSetMetaDataModel _metaData;
            private readonly SubscriptionModel _subscriptionInfo;
            private readonly ILogger _logger;
        }

        // Services
        private readonly ISubscriptionManager _subscriptions;
        private readonly IWriterGroupMessageEmitter _emitter;
        private readonly IWriterGroupStateReporter _state;
        private readonly ILogger _logger;
        private readonly ConcurrentDictionary<string, DataSetWriterSubscription> _writers;
        private readonly IWriterGroupDiagnostics _diagnostics;
    }
}
