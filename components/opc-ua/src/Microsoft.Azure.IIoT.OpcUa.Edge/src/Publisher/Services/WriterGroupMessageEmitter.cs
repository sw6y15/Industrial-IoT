// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Services {
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Core;
    using Microsoft.Azure.IIoT.Messaging;
    using Serilog;
    using Prometheus;
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Dataflow;
    using System.Collections.Generic;
    using System.Globalization;

    /// <summary>
    /// Writer group processing engine
    /// </summary>
    public class WriterGroupMessageEmitter : IWriterGroupMessageEmitter,
        IDisposable {

        /// <inheritdoc/>
        public string WriterGroupId { get; set; }

        /// <inheritdoc/>
        public uint? MaxNetworkMessageSize {
            get => _maxEncodedMessageSize ?? 256 * 1024;
            set {
                var prev = _maxEncodedMessageSize ?? 256 * 1024;
                var now = value ?? 256 * 1024;
                if (now <= 0) {
                    return;
                }
                if (prev != now) {
                    _maxEncodedMessageSize = now;
                    ResetDataFlowEngine();
                }
            }
        }

        /// <inheritdoc/>
        public TimeSpan? PublishingInterval {
            get => _publishingInterval;
            set {
                var prev = _publishingInterval ?? TimeSpan.Zero;
                var now = value ?? TimeSpan.Zero;
                if (now < TimeSpan.Zero) {
                    return;
                }
                if (prev != now) {
                    _publishingInterval = now;
                    ResetDataFlowEngine();
                }
            }
        }

        /// <inheritdoc/>
        public int? BatchSize {
            get => _batchSize ?? 1;
            set {
                var prev = _batchSize ?? 1;
                var now = value ?? 1;
                if (now < 0) {
                    return;
                }
                if (prev != now) {
                    _batchSize = now;
                    ResetDataFlowEngine();
                }
            }
        }

        /// <inheritdoc/>
        public MessageEncoding? Encoding {
            get => _encoding ?? MessageEncoding.Json;
            set {
                var prev = _encoding ?? MessageEncoding.Json;
                var now = value ?? MessageEncoding.Json;
                if (value == null || _encoders.Keys.Any(s => now.Matches(s))) {
                    _encoding = now;
                    ResetDataFlowEngine();
                }
            }
        }

        /// <inheritdoc/>
        public MessageSchema? Schema {
            get => _schema ?? MessageSchema.PubSub;
            set {
                var prev = _schema ?? MessageSchema.PubSub;
                var now = value ?? MessageSchema.PubSub;
                if (prev != now && _encoders.Keys.Any(s => now.Matches(s))) {
                    _schema = now;
                    ResetDataFlowEngine();
                }
            }
        }

        /// <inheritdoc/>
        public NetworkMessageContentMask? MessageContentMask {
            get => _networkMessageContentMask ?? NetworkMessageContentMask.Default;
            set {
                var prev = _networkMessageContentMask ?? NetworkMessageContentMask.Default;
                var now = value ?? NetworkMessageContentMask.Default;
                if (prev != now && _encoders.Keys.Any(s => now.Matches(s))) {
                    _networkMessageContentMask = now;
                    ResetDataFlowEngine();
                }
            }
        }

        /// <inheritdoc/>
        public DataSetOrderingType? DataSetOrdering {
            get => _dataSetOrdering ?? DataSetOrderingType.AscendingWriterId;
            set {
                var prev = _dataSetOrdering ?? DataSetOrderingType.AscendingWriterId;
                var now = value ?? DataSetOrderingType.AscendingWriterId;
                if (prev != now) {
                    _dataSetOrdering = now;
                    ResetDataFlowEngine();
                }
            }
        }

        /// <inheritdoc/>
        public string HeaderLayoutUri {
            get => _headerLayoutUri;
            set {
                // Check schema exists
                if (_headerLayoutUri != value) {
                    _headerLayoutUri = value;
                    ResetDataFlowEngine();
                }
            }
        }

        /// <inheritdoc/>
        public List<double> PublishingOffset { get; set; }

        /// <summary>
        /// Create writer group processor
        /// </summary>
        /// <param name="encoders"></param>
        /// <param name="events"></param>
        /// <param name="diagnostics"></param>
        /// <param name="logger"></param>
        public WriterGroupMessageEmitter(IEventClient events, IWriterGroupDiagnostics diagnostics,
            IEnumerable<INetworkMessageEncoder> encoders, ILogger logger) {

            _events = events ?? throw new ArgumentNullException(nameof(events));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _diagnostics = diagnostics ?? throw new ArgumentNullException(nameof(diagnostics));

            _encoders = encoders?.ToDictionary(e => e.MessageSchema, e => e) ??
                throw new ArgumentNullException(nameof(encoders));
            if (_encoders.Count == 0) {
                // Add at least one encoder
                _encoders.Add(MessageSchemaTypes.NetworkMessageUadp,
                    new UadpNetworkMessageEncoder());
            }
        }

        /// <inheritdoc/>
        public void Enqueue(DataSetWriterMessageModel message) {
            try {
                // Lazy re-create
                if (_engine == null) {
                    if (_disposed) {
                        throw new ObjectDisposedException(nameof(WriterGroupMessageEmitter));
                    }
                    _engine = new DataFlowProcessingEngine(this);
                }
                _engine.Enqueue(message);
            }
            catch (Exception ex) {
                _logger.Debug(ex, "Failed to produce message");
            }
        }

        /// <inheritdoc/>
        public void Dispose() {
            // Stop
            _disposed = true;
            var cur = _engine;
            cur?.Dispose();
        }

        /// <summary>
        /// Send message
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        private async Task EmitNetworkMessageAsync(NetworkMessageModel message) {
            try {
                using (kSendingDuration.NewTimer()) {
                    await _events.SendEventAsync(message.Body,
                        message.ContentType, message.MessageSchema, message.ContentEncoding);
                }
                _diagnostics.ReportNetworkMessageSent(WriterGroupId);
                kMessagesSent.WithLabels(_iotHubMessageSinkGuid, _iotHubMessageSinkStartTime).Inc();
            }
            catch (Exception ex) {
                _logger.Error(ex, "Error while sending messages to IoT Hub.");
                // we do not set the block into a faulted state.
            }
        }

        /// <summary>
        /// Unset flow engine
        /// </summary>
        private void ResetDataFlowEngine() {
            var cur = _engine;
            _engine = null; // Recreates on new message
            cur?.Dispose(); // Drains messages into sender
        }

        /// <summary>
        /// Data flow processing engine
        ///
        /// TODO: Consider using rx.net instead for more control over time windows, etc.
        /// </summary>
        private sealed class DataFlowProcessingEngine {

            /// <summary>
            /// Create engine
            /// </summary>
            /// <param name="outer"></param>
            internal DataFlowProcessingEngine(WriterGroupMessageEmitter outer) {
                _cts = new CancellationTokenSource();
                _outer = outer ?? throw new ArgumentNullException(nameof(outer));
                _logger = _outer._logger?.ForContext<WriterGroupDataCollector>() ??
                    throw new ArgumentNullException(nameof(_logger));

                // Snapshot the current configuration
                _publishingInterval = outer.PublishingInterval ?? TimeSpan.Zero;
                var messageSchema = MessageSchemaEx.ToMessageSchemaMimeType(
                     _outer.Schema, _outer.Encoding); // TODO
                if (messageSchema == null || !_outer._encoders.TryGetValue(messageSchema,
                    out _encoder)) {
                    // Should not happen, but set a default
                    _encoder = new UadpNetworkMessageEncoder();
                }
                _maxNetworkMessageSize = _outer.MaxNetworkMessageSize.Value;
                var batchSize = _outer.BatchSize ?? 1;

                // Input
                _batch = new BatchBlock<DataSetWriterMessageModel>(
                    batchSize,
                    new GroupingDataflowBlockOptions {
                        CancellationToken = _cts.Token,
                        EnsureOrdered = true
                    });

                // Encoder
                _encode = new TransformManyBlock<DataSetWriterMessageModel[], NetworkMessageModel>(
                    EncodeAsync,
                    new ExecutionDataflowBlockOptions {
                        BoundedCapacity = batchSize, // TODO: Should be configurable
                        SingleProducerConstrained = true,
                        EnsureOrdered = true,
                        CancellationToken = _cts.Token
                    });

                // Emit message - TODO: This should be in the outer and invoked from here because
                // we need to ensure correct ordering of sent messages even if engine is torn down
                // and recreated.
                _emit = new ActionBlock<NetworkMessageModel>(
                    SendAsync,
                    new ExecutionDataflowBlockOptions {
                        BoundedCapacity = 10, // TODO: Should be configurable
                        MaxDegreeOfParallelism = 1,
                        EnsureOrdered = true,
                        CancellationToken = _cts.Token
                    });

                // Link it all up
                _batch.LinkTo(_encode, new DataflowLinkOptions {
                    PropagateCompletion = true
                });
                _encode.LinkTo(_emit, new DataflowLinkOptions {
                    PropagateCompletion = true
                });

                _publishingIntervalTimer = new Timer(OnPublishingTimerExpired);
                _outer._diagnostics.BeforeDiagnosticsSending +=
                    OnUpdateWriterGroupDiagnostics;
            }

            /// <summary>
            /// process message
            /// </summary>
            /// <param name="message"></param>
            public void Enqueue(DataSetWriterMessageModel message) {
                while (!_cts.IsCancellationRequested) {
                    if (_batch.Post(message)) {
                        ReArmPublishingTimer();
                        break;  // Once encoded
                    }
                    //
                    // Perform back pressure
                    //
                    // This is not what we really want - we want to apply
                    // back pressure at the message level, but for that
                    // we will need an ack on the message.
                    //
                    Thread.Sleep(100);
                }
            }

            /// <inheritdoc/>
            public void Dispose() {
                try {
                    // Stop
                    _batch.Complete();
                    _cts.Cancel();
                }
                catch {
                    // Nothing...
                }
                finally {
                    _outer._diagnostics.BeforeDiagnosticsSending -=
                        OnUpdateWriterGroupDiagnostics;
                    _publishingIntervalTimer.Dispose();
                    _cts.Dispose();
                }
            }

            /// <summary>
            /// Encode messages
            /// </summary>
            /// <param name="messages"></param>
            /// <returns></returns>
            private IEnumerable<NetworkMessageModel> EncodeAsync(
                DataSetWriterMessageModel[] messages) {
                if (_batch.BatchSize == 1) {
                    return _encoder.Encode(_outer.WriterGroupId, messages,
                        _outer.HeaderLayoutUri, _outer.MessageContentMask,
                        _outer.DataSetOrdering, (int)_maxNetworkMessageSize);
                }
                return _encoder.EncodeBatch(_outer.WriterGroupId, messages,
                    _outer.HeaderLayoutUri, _outer.MessageContentMask,
                    _outer.DataSetOrdering, (int)_maxNetworkMessageSize);
            }

            /// <summary>
            /// Send message
            /// </summary>
            /// <param name="message"></param>
            /// <returns></returns>
            private async Task SendAsync(NetworkMessageModel message) {
                if (message != null) {
                    await _outer.EmitNetworkMessageAsync(message);
                    // message published re-arm publishing timer
                    ReArmPublishingTimer();
                }
            }

            /// <summary>
            /// Re-arm the publishing timeout timer if not already armed
            /// </summary>
            private void ReArmPublishingTimer() {
                if (!_armed && _publishingInterval > TimeSpan.Zero) {
                    // Start timer
                    _armed = true;
                    _publishingIntervalTimer.Change(
                        _publishingInterval, Timeout.InfiniteTimeSpan);
                    // Now timer is armed - will trigger batch once expired.
                }
            }

            /// <summary>
            /// Trigger publishing of batched block messages
            /// </summary>
            /// <param name="state"></param>
            private void OnPublishingTimerExpired(object state) {
                _batch.TriggerBatch();
                _armed = false; // once sent we re-arm the interval timer
            }

            /// <summary>
            /// Triggers updating diagnostics
            /// </summary>
            /// <param name="sender"></param>
            /// <param name="evt"></param>
            private void OnUpdateWriterGroupDiagnostics(object sender, EventArgs evt) {
                var diagnostics = (IWriterGroupDiagnostics)sender;
                var writerGroupId = _outer.WriterGroupId;

                diagnostics.ReportBatchedDataSetMessageCount(writerGroupId,
                    _batch.OutputCount);
                diagnostics.ReportDataSetMessagesReadyToEncode(writerGroupId,
                    _encode.InputCount);
                diagnostics.ReportEncodedNetworkMessages(writerGroupId,
                    _encode.OutputCount);
                diagnostics.ReportNetworkMessagesReadyToSend(writerGroupId,
                    _emit.InputCount);

                diagnostics.ReportEncoderNetworkMessagesProcessedCount(writerGroupId,
                    _encoder.MessageSchema, _encoder.MessagesProcessedCount);
                diagnostics.ReportEncoderNotificationsDroppedCount(writerGroupId,
                    _encoder.MessageSchema, _encoder.NotificationsDroppedCount);
                diagnostics.ReportEncoderNotificationsProcessedCount(writerGroupId,
                    _encoder.MessageSchema, _encoder.NotificationsProcessedCount);
                diagnostics.ReportEncoderAvgNotificationsPerMessage(writerGroupId,
                    _encoder.MessageSchema, _encoder.AvgNotificationsPerMessage);
                diagnostics.ReportEncoderAvgNetworkMessageSize(writerGroupId,
                    _encoder.MessageSchema, _encoder.AvgMessageSize, _maxNetworkMessageSize);
            }

            private readonly TransformManyBlock<DataSetWriterMessageModel[], NetworkMessageModel> _encode;
            private readonly BatchBlock<DataSetWriterMessageModel> _batch;
            private readonly ActionBlock<NetworkMessageModel> _emit;
            private readonly ILogger _logger;
            private readonly CancellationTokenSource _cts;
            private readonly Timer _publishingIntervalTimer;
            private readonly WriterGroupMessageEmitter _outer;
            private readonly INetworkMessageEncoder _encoder;
            private readonly TimeSpan _publishingInterval;
            private readonly uint _maxNetworkMessageSize;
            private bool _armed;
        }

        private readonly Dictionary<string, INetworkMessageEncoder> _encoders;
        private readonly IWriterGroupDiagnostics _diagnostics;
        private readonly IEventClient _events;
        private readonly ILogger _logger;

        private DataFlowProcessingEngine _engine;
        private bool _disposed;
        private uint? _maxEncodedMessageSize;
        private int? _batchSize;
        private TimeSpan? _publishingInterval;
        private string _headerLayoutUri;
        private NetworkMessageContentMask? _networkMessageContentMask;
        private DataSetOrderingType? _dataSetOrdering;
        private MessageEncoding? _encoding;
        private MessageSchema? _schema;
        private static readonly GaugeConfiguration kGaugeConfig = new GaugeConfiguration {
            LabelNames = new[] { "runid", "triggerid" }
        };

        private readonly string _iotHubMessageSinkGuid = Guid.NewGuid().ToString();
        private readonly string _iotHubMessageSinkStartTime =
            DateTime.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss.FFFFFFFK", CultureInfo.InvariantCulture);
        private static readonly Histogram kSendingDuration = Metrics.CreateHistogram(
            "iiot_edge_publisher_messages_duration", "Histogram of message sending durations");
        private static readonly Gauge kMessagesSent = Metrics.CreateGauge(
            "iiot_edge_publisher_messages", "Number of messages sent to IotHub", kGaugeConfig);
    }
}
