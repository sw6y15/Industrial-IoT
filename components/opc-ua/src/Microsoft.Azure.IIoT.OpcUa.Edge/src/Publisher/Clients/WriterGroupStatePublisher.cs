// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Clients {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Registry.Models;
    using Microsoft.Azure.IIoT.OpcUa.Registry;
    using Microsoft.Azure.IIoT.Serializers;
    using Microsoft.Azure.IIoT.Tasks;
    using Microsoft.Azure.IIoT.Module;
    using Serilog;
    using System;
    using System.Threading.Tasks;

    /// <summary>
    /// Send data set writer state events
    /// </summary>
    public sealed class WriterGroupStatePublisher : IWriterGroupStateReporter {

        /// <summary>
        /// Create listener
        /// </summary>
        /// <param name="events"></param>
        /// <param name="serializer"></param>
        /// <param name="processor"></param>
        /// <param name="logger"></param>
        public WriterGroupStatePublisher(IEventEmitter events, IJsonSerializer serializer,
            ITaskProcessor processor, ILogger logger) {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _processor = processor ?? throw new ArgumentNullException(nameof(processor));
            _events = events ?? throw new ArgumentNullException(nameof(events));
            _logger = new WriterGroupStateLogger(logger);
        }

        /// <inheritdoc/>
        public void OnDataSetEventStateChange(string dataSetWriterId,
            PublishedDataSetItemStateModel state) {
            _logger.OnDataSetEventStateChange(dataSetWriterId, state);
            var ev = new WriterGroupStateEventModel {
                WriterGroupId = PublisherRegistryEx.ToWriterGroupId(_events.DeviceId),
                DataSetWriterId = dataSetWriterId,
                EventType = PublisherStateEventType.PublishedItem,
                LastResult = state?.LastResult,
                TimeStamp = DateTime.UtcNow
            };
            _processor.TrySchedule(() => SendAsync(ev));
        }

        /// <inheritdoc/>
        public void OnDataSetVariableStateChange(string dataSetWriterId,
            string variableId, PublishedDataSetItemStateModel state) {
            _logger.OnDataSetVariableStateChange(dataSetWriterId, variableId, state);
            var ev = new WriterGroupStateEventModel {
                WriterGroupId = PublisherRegistryEx.ToWriterGroupId(_events.DeviceId),
                DataSetWriterId = dataSetWriterId,
                EventType = PublisherStateEventType.PublishedItem,
                LastResult = state?.LastResult,
                TimeStamp = DateTime.UtcNow,
                PublishedVariableId = variableId
            };
            _processor.TrySchedule(() => SendAsync(ev));
        }

        /// <inheritdoc/>
        public void OnDataSetWriterStateChange(string dataSetWriterId,
            PublishedDataSetSourceStateModel state) {
            _logger.OnDataSetWriterStateChange(dataSetWriterId, state);
            var ev = new WriterGroupStateEventModel {
                WriterGroupId = PublisherRegistryEx.ToWriterGroupId(_events.DeviceId),
                DataSetWriterId = dataSetWriterId,
                EventType = PublisherStateEventType.Source,
                LastResult = state?.LastResult,
                TimeStamp = DateTime.UtcNow,
            };
            _processor.TrySchedule(() => SendAsync(ev));
        }

        /// <summary>
        /// Send status
        /// </summary>
        /// <param name="state"></param>
        /// <returns></returns>
        private Task SendAsync(WriterGroupStateEventModel state) {
            return _events.SendEventAsync(
                _serializer.SerializeToBytes(state).ToArray(), ContentMimeType.Json,
                MessageSchemaTypes.WriterGroupEvents, "utf-8");
        }

        private readonly WriterGroupStateLogger _logger;
        private readonly IJsonSerializer _serializer;
        private readonly ITaskProcessor _processor;
        private readonly IEventEmitter _events;
    }
}