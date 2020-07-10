// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Services {
    using Microsoft.Azure.IIoT.Module;
    using Serilog;
    using Prometheus;
    using System;
    using System.Text;
    using System.Threading;

    /// <summary>
    /// Log writer group diganotics
    /// </summary>
    public sealed class WriterGroupDiagnostics : IWriterGroupDiagnostics, IDisposable {

        /// <inheritdoc/>
        public event EventHandler<EventArgs> BeforeDiagnosticsSending;
        /// <inheritdoc/>
        public event EventHandler<EventArgs> AfterDiagnosticsSending;

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

        /// <summary>
        /// Device id
        /// </summary>
        internal string DeviceId => _identity.DeviceId ?? "";

        /// <summary>
        /// Module id
        /// </summary>
        internal string ModuleId => _identity.ModuleId ?? "";

        /// <summary>
        /// Create writer group diagnostics logger
        /// </summary>
        /// <param name="identity"></param>
        /// <param name="logger"></param>
        public WriterGroupDiagnostics(IIdentity identity, ILogger logger) {
            _identity = identity ?? throw new ArgumentNullException(nameof(identity));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _diagnosticsOutputTimer = new Timer(DiagnosticsOutputTimer_Elapsed);
        }

        /// <inheritdoc/>
        public void ReportDataSetWriterSubscriptionNotifications(string writerGroupId,
            string dataSetWriterId, int count) {
            Interlocked.Add(ref _valueChangesCount, count);
            Interlocked.Increment(ref _dataChangesCount);
            kDataChangesCount.WithLabels(DeviceId, ModuleId, writerGroupId)
                .Set(_dataChangesCount);
            kValueChangesCount.WithLabels(DeviceId, ModuleId, writerGroupId)
                .Set(_valueChangesCount);
        }

        /// <inheritdoc/>
        public void ReportBatchedDataSetMessageCount(string writerGroupId, int count) {
            Interlocked.Exchange(ref _batchedDataSetMessageCount, count);
        }

        /// <inheritdoc/>
        public void ReportDataSetMessagesReadyToEncode(string writerGroupId, int count) {
            Interlocked.Exchange(ref _messagesReadyToEncodeCount, count);
        }

        /// <inheritdoc/>
        public void ReportEncodedNetworkMessages(string writerGroupId, int count) {
            Interlocked.Exchange(ref _encodedNetworkMessageCount, count);
        }

        /// <inheritdoc/>
        public void ReportNetworkMessagesReadyToSend(string writerGroupId, int count) {
            Interlocked.Exchange(ref _readyToSendCount, count);
            kIoTHubQueueBuffer.WithLabels(DeviceId, ModuleId, writerGroupId)
                .Set(_readyToSendCount);
        }

        /// <inheritdoc/>
        public void ReportNetworkMessageSent(string writerGroupId) {
            Interlocked.Increment(ref _sentMessagesCount);
            kSentMessagesCount.WithLabels(DeviceId, ModuleId, writerGroupId)
                .Set(_sentMessagesCount);
        }

        /// <inheritdoc/>
        public void ReportConnectionRetry(string writerGroupId, string dataSetWriterId) {
            Interlocked.Increment(ref _numberOfConnectionRetries);
            kNumberOfConnectionRetries.WithLabels(DeviceId, ModuleId, writerGroupId)
                .Set(_numberOfConnectionRetries);
        }

        /// <inheritdoc/>
        public void ReportEncoderNetworkMessagesProcessedCount(string writerGroupId,
            string messageScheme, uint count) {
            Interlocked.Exchange(ref _messagesProcessedCount, count);
            kMessagesProcessedCount.WithLabels(DeviceId, ModuleId, writerGroupId)
                .Set(_messagesProcessedCount);
        }

        /// <inheritdoc/>
        public void ReportEncoderNotificationsDroppedCount(string writerGroupId,
            string messageScheme, uint count) {
            Interlocked.Exchange(ref _notificationsDroppedCount, count);
            kNotificationsDroppedCount.WithLabels(DeviceId, ModuleId, writerGroupId)
                .Set(_notificationsDroppedCount);
        }

        /// <inheritdoc/>
        public void ReportEncoderNotificationsProcessedCount(string writerGroupId,
            string messageScheme, uint count) {
            Interlocked.Exchange(ref _notificationsProcessedCount, count);
            kNotificationsProcessedCount.WithLabels(DeviceId, ModuleId, writerGroupId)
                .Set(_notificationsProcessedCount);
        }

        /// <inheritdoc/>
        public void ReportEncoderAvgNotificationsPerMessage(string writerGroupId,
            string messageScheme, double average) {
            Interlocked.Exchange(ref _avgNotificationsPerMessage, average);
            kNotificationsPerMessageAvg.WithLabels(DeviceId, ModuleId, writerGroupId)
                .Set(_avgNotificationsPerMessage);
        }

        /// <inheritdoc/>
        public void ReportEncoderAvgNetworkMessageSize(string writerGroupId,
            string messageScheme, double average, uint max) {
            Interlocked.Exchange(ref _avgMessageSize, average);
            Interlocked.Exchange(ref _maxEncodedMessageSize, max);
            kMessageSizeAvg.WithLabels(DeviceId, ModuleId, writerGroupId)
                .Set(_avgMessageSize);
            kChunkSizeAvg.WithLabels(DeviceId, ModuleId, writerGroupId)
                .Set(_avgMessageSize / (4 * 1024));
        }

        /// <inheritdoc/>
        public void Dispose() {
            _diagnosticsOutputTimer.Dispose();
        }

        /// <summary>
        /// Diagnostics timer
        /// </summary>
        /// <param name="state"></param>
        private void DiagnosticsOutputTimer_Elapsed(object state) {
            BeforeDiagnosticsSending.Invoke(this, EventArgs.Empty);

            var totalDuration = _diagnosticStart != DateTime.MinValue ? (DateTime.UtcNow - _diagnosticStart).TotalSeconds : 0;
            var valueChangesPerSec = _valueChangesCount / totalDuration;
            var dataChangesPerSec = _dataChangesCount / totalDuration;
            var sentMessagesPerSec = totalDuration > 0 ? _sentMessagesCount / totalDuration : 0;
            var messageSizeAveragePercent = Math.Round(_avgMessageSize / _maxEncodedMessageSize * 100);
            var messageSizeAveragePercentFormatted = $"({messageSizeAveragePercent}%)";
            var chunkSizeAverage = _avgMessageSize / (4 * 1024);
            var estimatedMsgChunksPerDay = Math.Ceiling(chunkSizeAverage) * sentMessagesPerSec * 60 * 60 * 24;

            var diagInfo = new StringBuilder();
            diagInfo.AppendLine();
            diagInfo.AppendLine("  DIAGNOSTICS INFORMATION for          : {host}");
            diagInfo.AppendLine("  # Ingestion duration                 : {duration,14:dd\\:hh\\:mm\\:ss} (dd:hh:mm:ss)");
            var dataChangesPerSecFormatted = _dataChangesCount > 0 && totalDuration > 0 ? $"({dataChangesPerSec:0.##}/s)" : "";
            diagInfo.AppendLine("  # Ingress DataChanges (from OPC)     : {dataChangesCount,14:n0} {dataChangesPerSecFormatted}");
            var valueChangesPerSecFormatted = _valueChangesCount > 0 && totalDuration > 0 ? $"({valueChangesPerSec:0.##}/s)" : "";
            diagInfo.AppendLine("  # Ingress ValueChanges (from OPC)    : {valueChangesCount,14:n0} {valueChangesPerSecFormatted}");

            diagInfo.AppendLine("  # Ingress BatchBlock buffer size     : {batchDataSetMessageBlockOutputCount,14:0}");
            diagInfo.AppendLine("  # Encoding Block input/output size   : {encodingBlockInputCount,14:0} | {encodingBlockOutputCount:0}");
            diagInfo.AppendLine("  # Encoder Notifications processed    : {notificationsProcessedCount,14:n0}");
            diagInfo.AppendLine("  # Encoder Notifications dropped      : {notificationsDroppedCount,14:n0}");
            diagInfo.AppendLine("  # Encoder IoT Messages processed     : {messagesProcessedCount,14:n0}");
            diagInfo.AppendLine("  # Encoder avg Notifications/Message  : {notificationsPerMessage,14:0}");
            diagInfo.AppendLine("  # Encoder avg IoT Message body size  : {messageSizeAverage,14:n0} {messageSizeAveragePercentFormatted}");
            diagInfo.AppendLine("  # Encoder avg IoT Chunk (4 KB) usage : {chunkSizeAverage,14:0.#}");
            diagInfo.AppendLine("  # Estimated IoT Chunks (4 KB) per day: {estimatedMsgChunksPerDay,14:n0}");
            diagInfo.AppendLine("  # Outgress input buffer count        : {sinkBlockInputCount,14:n0}");

            var sentMessagesPerSecFormatted = _sentMessagesCount > 0 && totalDuration > 0 ? $"({sentMessagesPerSec:0.##}/s)" : "";
            diagInfo.AppendLine("  # Outgress IoT message count         : {messageSinkSentMessagesCount,14:n0} {sentMessagesPerSecFormatted}");
            diagInfo.AppendLine("  # Connection retries                 : {connectionRetries,14:0}");

            _logger.Information(diagInfo.ToString(),
                DeviceId, ModuleId,
                TimeSpan.FromSeconds(totalDuration),
                _dataChangesCount, dataChangesPerSecFormatted,
                _valueChangesCount, valueChangesPerSecFormatted,
                _batchedDataSetMessageCount,
                _messagesReadyToEncodeCount, _encodedNetworkMessageCount,
                _notificationsProcessedCount,
                _notificationsDroppedCount,
                _messagesProcessedCount,
                _avgNotificationsPerMessage,
                _avgMessageSize, messageSizeAveragePercentFormatted,
                chunkSizeAverage,
                estimatedMsgChunksPerDay,
                _readyToSendCount,
                _sentMessagesCount, sentMessagesPerSecFormatted,
                _numberOfConnectionRetries);

            AfterDiagnosticsSending.Invoke(this, EventArgs.Empty);
        }

        // Diagnostics
        private readonly DateTime _diagnosticStart = DateTime.UtcNow;
        private readonly Timer _diagnosticsOutputTimer;

        private TimeSpan? _diagnosticsInterval;
        private double _avgNotificationsPerMessage;
        private double _avgMessageSize;
        private long _readyToSendCount;
        private long _valueChangesCount;
        private long _dataChangesCount;
        private long _sentMessagesCount;
        private long _batchedDataSetMessageCount;
        private long _messagesReadyToEncodeCount;
        private long _encodedNetworkMessageCount;
        private long _numberOfConnectionRetries;
        private long _notificationsProcessedCount;
        private long _notificationsDroppedCount;
        private long _messagesProcessedCount;
        private long _maxEncodedMessageSize;

        private static readonly GaugeConfiguration kGaugeConfig = new GaugeConfiguration {
            LabelNames = new[] { "deviceid", "module", "triggerid" }
        };
        private static readonly Gauge kValueChangesCount = Metrics.CreateGauge(
            "iiot_edge_publisher_value_changes",
            "Opc ValuesChanges delivered for processing", kGaugeConfig);
        private static readonly Gauge kDataChangesCount = Metrics.CreateGauge(
            "iiot_edge_publisher_data_changes",
            "Opc DataChanges delivered for processing", kGaugeConfig);
        private static readonly Gauge kIoTHubQueueBuffer = Metrics.CreateGauge(
            "iiot_edge_publisher_iothub_queue_size",
            "IoT messages queued sending", kGaugeConfig);
        private static readonly Gauge kSentMessagesCount = Metrics.CreateGauge(
            "iiot_edge_publisher_sent_iot_messages",
            "IoT messages sent to hub", kGaugeConfig);
        private static readonly Gauge kNumberOfConnectionRetries = Metrics.CreateGauge(
            "iiot_edge_publisher_connection_retries",
            "OPC UA connect retries", kGaugeConfig);
        private static readonly Gauge kNotificationsProcessedCount = Metrics.CreateGauge(
            "iiot_edge_publisher_encoded_notifications",
            "publisher engine encoded opc notifications count", kGaugeConfig);
        private static readonly Gauge kNotificationsDroppedCount = Metrics.CreateGauge(
            "iiot_edge_publisher_dropped_notifications",
            "publisher engine dropped opc notifications count", kGaugeConfig);
        private static readonly Gauge kMessagesProcessedCount = Metrics.CreateGauge(
            "iiot_edge_publisher_processed_messages",
            "publisher engine processed iot messages count", kGaugeConfig);
        private static readonly Gauge kNotificationsPerMessageAvg = Metrics.CreateGauge(
            "iiot_edge_publisher_notifications_per_message_average",
            "publisher engine opc notifications per iot message average", kGaugeConfig);
        private static readonly Gauge kMessageSizeAvg = Metrics.CreateGauge(
            "iiot_edge_publisher_encoded_message_size_average",
            "publisher engine iot message encoded body size average", kGaugeConfig);
        private static readonly Gauge kChunkSizeAvg = Metrics.CreateGauge(
            "iiot_edge_publisher_chunk_size_average",
            "IoT Hub chunk size average", kGaugeConfig);

        private readonly IIdentity _identity;
        private readonly ILogger _logger;
    }
}
