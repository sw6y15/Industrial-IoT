// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Services {
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Protocol;
    using Microsoft.Azure.IIoT.OpcUa.Protocol.Models;
    using Microsoft.Azure.IIoT.OpcUa.Core;
    using Microsoft.Azure.IIoT.Module;
    using Microsoft.Azure.IIoT.Messaging;
    using Microsoft.Azure.IIoT.Utils;
    using Serilog;
    using Prometheus;
    using System;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.Globalization;

    /// <summary>
    /// Writer group processing engine
    /// </summary>
    public class WriterGroupProcessingEngine : IWriterGroupProcessingEngine,
        IDisposable {

        /// <summary>
        /// Publisher id
        /// </summary>
        internal string PublisherId {
            get {
                var moduleId = _identity.ModuleId;
                var deviceId = _identity.DeviceId;
                if (string.IsNullOrEmpty(moduleId)) {
                    if (string.IsNullOrEmpty(deviceId)) {
                        return null;
                    }
                    return deviceId;
                }
                return deviceId + "_" + moduleId;
            }
        }

        /// <inheritdoc/>
        public string WriterGroupId {
            get {
                var writerGroupId = _writerGroupId;
                if (string.IsNullOrEmpty(writerGroupId)) {
                    writerGroupId = PublisherId;
                }
                return writerGroupId;
            }
            set => _writerGroupId = value;
        }

        /// <inheritdoc/>
        public uint? MaxNetworkMessageSize {
            get => _maxEncodedMessageSize ?? 256 * 1024;
            set {
                if (!value.HasValue || value.Value > 0) {
                    _maxEncodedMessageSize = value;
                }
            }
        }

        /// <inheritdoc/>
        public TimeSpan? PublishingInterval {
            get => _publishingInterval;
            set {
                if (_publishingInterval != value) {
                    _engine?.SetBatchTriggerInterval(value);
                    _publishingInterval = value;
                }
            }
        }

        /// <inheritdoc/>
        public int? BatchSize {
            get => _batchSize ?? 1;
            set {
                if (!value.HasValue || value.Value > 0) {
                    _batchSize = value;
                }
            }
        }

        /// <inheritdoc/>
        public TimeSpan? DiagnosticsInterval {
            get => _diagnosticsInterval;
            set {
                if (_diagnosticsInterval != value) {
                    if (value == null) {
                        _diagnosticsOutputTimer.Change(Timeout.Infinite, Timeout.Infinite);
                    }
                    else {
                        _diagnosticsOutputTimer.Change(value.Value, value.Value);
                    }
                    _diagnosticsInterval = value;
                }
            }
        }

        /// <inheritdoc/>
        public string MessageSchema {
            get => _encoder?.MessageScheme;
            set {
                // Set new decoder
                if (value != null && _encoders.TryGetValue(value, out var encoder)) {
                    _encoder = encoder;
                }
                else {
                    // Unset encoder
                    _encoder = null;
                }
            }
        }

        /// <inheritdoc/>
        public string HeaderLayoutUri { get; set; }

        /// <inheritdoc/>
        public NetworkMessageContentMask? NetworkMessageContentMask { get; set; }

        /// <inheritdoc/>
        public uint? GroupVersion { get; set; }

        /// <inheritdoc/>
        public DataSetOrderingType? DataSetOrdering { get; set; }

        /// <inheritdoc/>
        public double? SamplingOffset { get; set; }

        /// <inheritdoc/>
        public List<double> PublishingOffset { get; set; }

        /// <inheritdoc/>
        public TimeSpan? KeepAliveTime { get; set; }

        /// <inheritdoc/>
        public byte? Priority { get; set; }

        /// <summary>
        /// Create writer group processor
        /// </summary>
        /// <param name="encoders"></param>
        /// <param name="state"></param>
        /// <param name="identity"></param>
        /// <param name="events"></param>
        /// <param name="subscriptions"></param>
        /// <param name="logger"></param>
        public WriterGroupProcessingEngine(IEventClient events, ISubscriptionManager subscriptions,
            IEnumerable<INetworkMessageEncoder> encoders, IWriterGroupStateReporter state,
            IIdentity identity, ILogger logger) {
            _state = state ?? throw new ArgumentNullException(nameof(state));
            _events = events ?? throw new ArgumentNullException(nameof(events));
            _identity = identity ?? throw new ArgumentNullException(nameof(identity));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _subscriptions = subscriptions ?? throw new ArgumentNullException(nameof(subscriptions));
            _encoders = encoders?.ToDictionary(e => e.MessageScheme, e => e) ??
                throw new ArgumentNullException(nameof(encoders));

            if (_encoders.Count == 0) {
                // Add at least one encoder
                _encoders.Add(MessageSchemaTypes.NetworkMessageUadp, new UadpNetworkMessageEncoder());
            }

            _engine = new DataFlowEngine(this);
            _writers = new ConcurrentDictionary<string, DataSetWriterSubscription>();
            _diagnosticsOutputTimer = new Timer(DiagnosticsOutputTimer_Elapsed);
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
                _engine.Dispose();
                RemoveAllWriters();
            }
            catch {
                // Nothing...
            }
            finally {
                _diagnosticsOutputTimer.Dispose();
                _writers.Clear();
            }
        }

        /// <summary>
        /// Message flow engine
        /// </summary>
        private sealed class DataFlowEngine {

            /// <summary>
            /// Input messages
            /// </summary>
            public int SourceMessageCount => _inputBlock.OutputCount;

            /// <summary>
            /// Encoding input
            /// </summary>
            public int EncodingInputCount => _encodingBlock.InputCount;

            /// <summary>
            /// Encoding output
            /// </summary>
            public int EncodingOutputCount => _encodingBlock.OutputCount;

            /// <summary>
            /// Sent pending
            /// </summary>
            public int SentPendingCount => _sinkBlock.InputCount;

            /// <summary>
            /// Sent messages
            /// </summary>
            public long SentCompleteCount => _sentCompleteCount;

            /// <summary>
            /// Create engine
            /// </summary>
            /// <param name="outer"></param>
            internal DataFlowEngine(WriterGroupProcessingEngine outer) {
                _outer = outer ?? throw new ArgumentNullException(nameof(outer));
                _logger = _outer._logger?.ForContext<WriterGroupProcessingEngine>() ??
                    throw new ArgumentNullException(nameof(_logger));
                _batchTriggerIntervalTimer = new Timer(BatchTriggerIntervalTimer_Elapsed);
                _cts = new CancellationTokenSource();

                // Input
                _inputBlock = new BatchBlock<DataSetWriterMessageModel>(
                    _outer.BatchSize.Value, new GroupingDataflowBlockOptions {
                        CancellationToken = _cts.Token,
                        EnsureOrdered = true
                    });

                // Encoder
                _encodingBlock = new TransformManyBlock<DataSetWriterMessageModel[], NetworkMessageModel>(
                    EncodeAsync,
                    new ExecutionDataflowBlockOptions {
                        SingleProducerConstrained = true,
                        EnsureOrdered = true,
                        CancellationToken = _cts.Token
                    });

                // Writer
                _sinkBlock = new ActionBlock<NetworkMessageModel>(
                    SendAsync,
                    new ExecutionDataflowBlockOptions {
                        MaxDegreeOfParallelism = 1,
                        EnsureOrdered = true,
                        CancellationToken = _cts.Token
                    });

                // Link it all up
                _inputBlock.LinkTo(_encodingBlock, new DataflowLinkOptions {
                    PropagateCompletion = true
                });
                _encodingBlock.LinkTo(_sinkBlock, new DataflowLinkOptions {
                    PropagateCompletion = true
                });
            }

            /// <inheritdoc/>
            public void SetBatchTriggerInterval(TimeSpan? value) {
                _batchTriggerInterval = value;
                if (_batchTriggerInterval == null || _batchTriggerInterval.Value == TimeSpan.Zero) {
                    _batchTriggerIntervalTimer.Change(Timeout.Infinite, Timeout.Infinite);
                }
            }

            /// <summary>
            /// process message
            /// </summary>
            /// <param name="message"></param>
            public void Enqueue(DataSetWriterMessageModel message) {
                _inputBlock.Post(message);
            }

            /// <inheritdoc/>
            public void Dispose() {
                try {
                    // Stop
                    _inputBlock.Complete();
                    _cts.Cancel();
                }
                catch {
                    // Nothing...
                }
                finally {
                    _batchTriggerIntervalTimer.Dispose();
                    _cts.Dispose();
                }
            }

            /// <summary>
            /// Batch trigger interval
            /// </summary>
            /// <param name="state"></param>
            private void BatchTriggerIntervalTimer_Elapsed(object state) {
                _inputBlock.TriggerBatch();
            }

            /// <summary>
            /// Encode messages
            /// </summary>
            /// <param name="messages"></param>
            /// <returns></returns>
            private IEnumerable<NetworkMessageModel> EncodeAsync(DataSetWriterMessageModel[] messages) {
                if (_batchTriggerInterval != null && _batchTriggerInterval.Value > TimeSpan.Zero) {
                    _batchTriggerIntervalTimer.Change(_batchTriggerInterval.Value, Timeout.InfiniteTimeSpan);
                }
                return _outer.EncodeMessages(messages);
            }

            /// <summary>
            /// Send messages
            /// </summary>
            /// <param name="message"></param>
            /// <returns></returns>
            private async Task SendAsync(NetworkMessageModel message) {
                if (message == null) {
                    return;
                }
                try {
                    using (kSendingDuration.NewTimer()) {
                        await _outer._events.SendEventAsync(message.Body,
                            message.ContentType, message.MessageSchema, message.ContentEncoding);
                    }
                    Interlocked.Increment(ref _sentCompleteCount);
                    kMessagesSent.WithLabels(_iotHubMessageSinkGuid, _iotHubMessageSinkStartTime).Inc();
                }
                catch (Exception ex) {
                    _logger.Error(ex, "Error while sending messages to IoT Hub.");
                    // we do not set the block into a faulted state.
                }
            }

            private readonly WriterGroupProcessingEngine _outer;
            private readonly ILogger _logger;
            private readonly Timer _batchTriggerIntervalTimer;
            private readonly BatchBlock<DataSetWriterMessageModel> _inputBlock;
            private readonly CancellationTokenSource _cts;
            private readonly TransformManyBlock<DataSetWriterMessageModel[], NetworkMessageModel> _encodingBlock;
            private readonly ActionBlock<NetworkMessageModel> _sinkBlock;
            private readonly string _iotHubMessageSinkGuid = Guid.NewGuid().ToString();
            private long _sentCompleteCount;
            private TimeSpan? _batchTriggerInterval;
            private readonly string _iotHubMessageSinkStartTime =
                DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss.FFFFFFFK", CultureInfo.InvariantCulture);
            private static readonly Histogram kSendingDuration = Metrics.CreateHistogram(
                "iiot_edge_publisher_messages_duration", "Histogram of message sending durations");
            private static readonly Gauge kMessagesSent = Metrics.CreateGauge(
                "iiot_edge_publisher_messages", "Number of messages sent to IotHub",
                    new GaugeConfiguration {
                        LabelNames = new[] { "runid", "timestamp_utc" }
                    });
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
            public DataSetWriterSubscription(WriterGroupProcessingEngine outer,
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

                sc.OnSubscriptionNotification += OnSubscriptionChangedAsync;
                sc.OnMonitoredItemStatusChange += OnMonitoredItemStatusAsync;
                sc.OnSubscriptionStatusChange += OnSubscriptionStatusAsync;

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

                    Subscription.OnSubscriptionNotification -= OnSubscriptionChangedAsync;
                    Subscription.OnMonitoredItemStatusChange -= OnMonitoredItemStatusAsync;
                    Subscription.OnSubscriptionStatusChange -= OnSubscriptionStatusAsync;

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
                        _outer.ProcessDataSetWriterNotification(_dataSetWriter, sequenceNumber, snapshot);
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
            private void OnSubscriptionStatusAsync(object sender, SubscriptionStatusModel e) {
                var state = new PublishedDataSetSourceStateModel {
                    LastResultChange = DateTime.UtcNow,
                    LastResult = e.Error,

                    // ...
                };
                _outer._state.OnDataSetWriterStateChange(_dataSetWriter.DataSetWriterId, state);
            }

            /// <summary>
            /// Process monitored item status change message
            /// </summary>
            /// <param name="sender"></param>
            /// <param name="e"></param>
            private void OnMonitoredItemStatusAsync(object sender, MonitoredItemStatusModel e) {
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
            private async void OnSubscriptionChangedAsync(object sender, SubscriptionNotificationModel e) {
                try {
                    var sequenceNumber = (uint)Interlocked.Increment(ref _currentSequenceNumber);
                    if (_keyFrameCount.HasValue && _keyFrameCount.Value != 0 &&
                        (sequenceNumber % _keyFrameCount.Value) == 0) {
                        var snapshot = await Try.Async(() => Subscription.GetSnapshotAsync());
                        if (snapshot != null) {
                            e = snapshot;
                        }
                    }
                    _outer.ProcessDataSetWriterNotification(_dataSetWriter, sequenceNumber, e);
                }
                catch (Exception ex) {
                    _logger.Error(ex, "Failed to process writer notification");
                }
            }

            private readonly System.Timers.Timer _keyframeTimer;
            private readonly System.Timers.Timer _metadataTimer;
            private readonly DataSetMetaDataModel _metaData;
            private readonly uint? _keyFrameCount;
            private long _currentSequenceNumber;
            private readonly WriterGroupProcessingEngine _outer;
            private readonly DataSetWriterModel _dataSetWriter;
            private readonly SubscriptionModel _subscriptionInfo;
            private readonly ILogger _logger;
        }

        /// <summary>
        /// handle subscription change messages
        /// </summary>
        /// <param name="dataSetWriter"></param>
        /// <param name="sequenceNumber"></param>
        /// <param name="notification"></param>
        private void ProcessDataSetWriterNotification(DataSetWriterModel dataSetWriter,
            uint sequenceNumber, SubscriptionNotificationModel notification) {
            try {
                var notifications = notification.Notifications.ToList();
                var message = new DataSetWriterMessageModel {
                    // TODO: Filter changes on the monitored items contained in the template
                    Notifications = notifications,
                    ServiceMessageContext = notification.ServiceMessageContext,
                    SubscriptionId = notification.SubscriptionId,
                    SequenceNumber = sequenceNumber,
                    ContentMask = (uint?)NetworkMessageContentMask,
                    PublisherId = PublisherId,
                    ApplicationUri = notification.ApplicationUri,
                    EndpointUrl = notification.EndpointUrl,
                    TimeStamp = notification.Timestamp,
                    Writer = dataSetWriter
                };
                Interlocked.Add(ref _valueChangesCount, notifications.Count);
                Interlocked.Increment(ref _dataChangesCount);
                _engine.Enqueue(message);
            }
            catch (Exception ex) {
                _logger.Debug(ex, "Failed to produce message");
            }
        }

        /// <summary>
        /// Encode dataset messages using the configured encoder
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        private IEnumerable<NetworkMessageModel> EncodeMessages(
            IEnumerable<DataSetWriterMessageModel> input) {
            var encoder = _encoder;
            if (encoder == null) {
                encoder = _encoders.First().Value; // Select first encoder
            }
            if (BatchSize.Value == 1) {
                return encoder.Encode(input, (int)MaxNetworkMessageSize.Value);
            }
            return encoder.EncodeBatch(input, (int)MaxNetworkMessageSize.Value);
        }

        // Services
        private readonly Dictionary<string, INetworkMessageEncoder> _encoders;
        private readonly ISubscriptionManager _subscriptions;
        private readonly IWriterGroupStateReporter _state;
        private readonly IEventClient _events;
        private readonly IIdentity _identity;
        private readonly ILogger _logger;

        // State
        private readonly ConcurrentDictionary<string, DataSetWriterSubscription> _writers;
        private INetworkMessageEncoder _encoder;
        private uint? _maxEncodedMessageSize;
        private int? _batchSize;
        private TimeSpan? _publishingInterval;
        private string _writerGroupId;
        private readonly DataFlowEngine _engine;

        /// <summary>
        /// Diagnostics timer
        /// </summary>
        /// <param name="state"></param>
        private void DiagnosticsOutputTimer_Elapsed(object state) {

            var writerGroupId = WriterGroupId;
            if (string.IsNullOrEmpty(writerGroupId)) {
                return;
            }

            var totalDuration = (DateTime.UtcNow - _diagnosticStart).TotalSeconds;
            var numberOfConnectionRetries = _writers.Values
                .Where(sc => sc.Subscription != null)
                .Select(sc => sc.Subscription)
                .Sum(sc => sc.NumberOfConnectionRetries);

            var notificationsProcessedCount = _encoders.Values.Sum(e => e.NotificationsProcessedCount);
            var messagesProcessedCount = _encoders.Values.Sum(e => e.MessagesProcessedCount);
            var notificationsDroppedCount = _encoders.Values.Sum(e => e.NotificationsDroppedCount);

            var avgNotificationsPerMessage = _encoder.AvgNotificationsPerMessage;
            var avgMessageSize = _encoder.AvgMessageSize;

            var diagInfo = new StringBuilder();
            diagInfo.AppendLine();
            diagInfo.AppendLine("   DIAGNOSTICS INFORMATION for         : {publisherId}:{writerGroupId}");
            diagInfo.AppendLine("   # Ingestion duration                : {duration,14:dd\\:hh\\:mm\\:ss} (dd:hh:mm:ss)");
            diagInfo.AppendLine("   # Ingress DataChanges (from OPC)    : {dataChangesCount,14:0}({dataChangesAverage:0.##}/s)");
            diagInfo.AppendLine("   # Ingress ValueChanges (from OPC)   : {valueChangesCount,14:0}({valueChangesAverage:0.##}/s)");
            diagInfo.AppendLine("   # Ingress BatchBlock buffer size    : {sourceMessageCount,14:0}");
            diagInfo.AppendLine("   # Encoding Block input/output size  : {encodingBlockInputCount,14:0} | {encodingBlockOutputCount:0}");
            diagInfo.AppendLine("   # Encoder Notifications processed   : {notificationsProcessedCount,14:0}");
            diagInfo.AppendLine("   # Encoder Notifications dropped     : {notificationsDroppedCount,14:0}");
            diagInfo.AppendLine("   # Encoder IoT Messages processed    : {messagesProcessedCount,14:0}");
            diagInfo.AppendLine("   # Encoder avg Notifications/Message : {notificationsPerMessage,14:0}");
            diagInfo.AppendLine("   # Encoder avg IoT Message body size : {messageSizeAverage,14:0}");
            diagInfo.AppendLine("   # Outgress input buffer count       : {sinkBlockInputCount,14:0}");
            diagInfo.AppendLine("   # Outgress IoT message count        : {messageSinkSentMessagesCount,14:0}({sentMessagesAverage:0.##}/s)");
            diagInfo.AppendLine("   # Connection retries                : {connectionRetries,14:0}");

            var dataChangesAverage = _dataChangesCount > 0 && totalDuration > 0 ?
                _dataChangesCount / totalDuration : 0;
            var valueChangesAverage = _valueChangesCount > 0 && totalDuration > 0 ?
                _valueChangesCount / totalDuration : 0;
            var sentMessagesAverage = _engine.SentCompleteCount > 0 && totalDuration > 0 ?
                _engine.SentCompleteCount / totalDuration : 0;

            _logger.Information(diagInfo.ToString(),
                PublisherId, writerGroupId,
                TimeSpan.FromSeconds(totalDuration),
                _dataChangesCount, dataChangesAverage,
                _valueChangesCount, valueChangesAverage,
                _engine?.SourceMessageCount,
                _engine?.EncodingInputCount, _engine?.EncodingOutputCount,
                notificationsProcessedCount,
                notificationsDroppedCount,
                messagesProcessedCount,
                avgNotificationsPerMessage,
                avgMessageSize,
                _engine?.SentPendingCount,
                _engine.SentCompleteCount, sentMessagesAverage,
                numberOfConnectionRetries);

            kDataChangesCount.WithLabels(PublisherId, writerGroupId)
                .Set(_dataChangesCount);
            kDataChangesPerSecond.WithLabels(PublisherId, writerGroupId)
                .Set(_valueChangesCount / totalDuration);
            kValueChangesCount.WithLabels(PublisherId, writerGroupId)
                .Set(_valueChangesCount);
            kValueChangesPerSecond.WithLabels(PublisherId, writerGroupId)
                .Set(_valueChangesCount / totalDuration);
            kNotificationsProcessedCount.WithLabels(PublisherId, writerGroupId)
                .Set(notificationsProcessedCount);
            kNotificationsDroppedCount.WithLabels(PublisherId, writerGroupId)
                .Set(notificationsDroppedCount);
            kMessagesProcessedCount.WithLabels(PublisherId, writerGroupId)
                .Set(messagesProcessedCount);
            kNotificationsPerMessageAvg.WithLabels(PublisherId, writerGroupId)
                .Set(avgNotificationsPerMessage);
            kMesageSizeAvg.WithLabels(PublisherId, writerGroupId)
                .Set(avgMessageSize);
            kIoTHubQueueBuffer.WithLabels(PublisherId, writerGroupId)
                .Set((long)_engine?.SentPendingCount);
            kSentMessagesCount.WithLabels(PublisherId, writerGroupId)
                .Set(_engine.SentCompleteCount);
            kNumberOfConnectionRetries.WithLabels(PublisherId, writerGroupId)
                .Set(numberOfConnectionRetries);
        }

        // Diagnostics
        private readonly DateTime _diagnosticStart = DateTime.UtcNow;
        private TimeSpan? _diagnosticsInterval;
        private readonly Timer _diagnosticsOutputTimer;
        private long _valueChangesCount;
        private long _dataChangesCount;

        private static readonly GaugeConfiguration kGaugeConfig = new GaugeConfiguration {
            LabelNames = new[] { "publisherid", "triggerid" }
        };

        private static readonly Gauge kValueChangesCount = Metrics.CreateGauge(
            "iiot_edge_publisher_value_changes", "Opc ValuesChanges delivered for processing", kGaugeConfig);
        private static readonly Gauge kValueChangesPerSecond = Metrics.CreateGauge(
            "iiot_edge_publisher_value_changes_per_second", "Opc ValuesChanges/second delivered for processing", kGaugeConfig);
        private static readonly Gauge kDataChangesCount = Metrics.CreateGauge(
            "iiot_edge_publisher_data_changes", "Opc DataChanges delivered for processing", kGaugeConfig);
        private static readonly Gauge kDataChangesPerSecond = Metrics.CreateGauge(
            "iiot_edge_publisher_data_changes_per_second", "Opc DataChanges/second delivered for processing", kGaugeConfig);
        private static readonly Gauge kIoTHubQueueBuffer = Metrics.CreateGauge(
            "iiot_edge_publisher_iothub_queue_size", "IoT messages queued sending", kGaugeConfig);
        private static readonly Gauge kSentMessagesCount = Metrics.CreateGauge(
            "iiot_edge_publisher_sent_iot_messages", "IoT messages sent to hub", kGaugeConfig);
        private static readonly Gauge kNumberOfConnectionRetries = Metrics.CreateGauge(
            "iiot_edge_publisher_connection_retries", "OPC UA connect retries", kGaugeConfig);

        private static readonly Gauge kNotificationsProcessedCount = Metrics.CreateGauge(
            "iiot_edge_publisher_encoded_notifications", "publisher engine encoded opc notifications count", kGaugeConfig);
        private static readonly Gauge kNotificationsDroppedCount = Metrics.CreateGauge(
            "iiot_edge_publisher_dropped_notifications", "publisher engine dropped opc notifications count", kGaugeConfig);
        private static readonly Gauge kMessagesProcessedCount = Metrics.CreateGauge(
            "iiot_edge_publisher_processed_messages", "publisher engine processed iot messages count", kGaugeConfig);
        private static readonly Gauge kNotificationsPerMessageAvg = Metrics.CreateGauge(
            "iiot_edge_publisher_notifications_per_message_average",
            "publisher engine opc notifications per iot message average", kGaugeConfig);
        private static readonly Gauge kMesageSizeAvg = Metrics.CreateGauge(
            "iiot_edge_publisher_encoded_message_size_average",
            "publisher engine iot message encoded body size average", kGaugeConfig);
    }
}
