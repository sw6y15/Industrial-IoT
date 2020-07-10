// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Services {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher;
    using Microsoft.Azure.IIoT.OpcUa.Registry.Models;
    using Microsoft.Azure.IIoT.OpcUa.Registry;
    using Serilog;
    using System;
    using System.Threading.Tasks;

    /// <summary>
    /// Process reported state change messages and update entites in registry
    /// </summary>
    public sealed class DataSetWriterStateSync : IWriterGroupStateProcessor {

        /// <summary>
        /// Create state processor service
        /// </summary>
        /// <param name="datasets"></param>
        /// <param name="logger"></param>
        public DataSetWriterStateSync(IDataSetWriterStateUpdate datasets, ILogger logger) {
            _datasets = datasets ?? throw new ArgumentNullException(nameof(datasets));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Handle state change
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public async Task OnWriterGroupStateChangeAsync(WriterGroupStateEventModel message) {

            var context = new PublisherOperationContextModel {
                Time = message.TimeStamp,
                AuthorityId = null // TODO
            };
            switch (message.EventType) {
                case PublisherStateEventType.Source:
                    var sourceState = new PublishedDataSetSourceStateModel {
                        LastResultChange = message.TimeStamp,
                        LastResult = message.LastResult,
                        EndpointState = message.EndpointState,
                        // ...
                    };

                    if (!string.IsNullOrEmpty(message.DataSetWriterId)) {
                        // Patch source state
                        await _datasets.UpdateDataSetWriterStateAsync(
                           message.DataSetWriterId, sourceState, context);
                        break;
                    }

                    _logger.Warning("Subscription event without dataset writer id");
                    break;

                case PublisherStateEventType.PublishedItem:
                    var itemState = new PublishedDataSetItemStateModel {
                        LastResultChange = message.TimeStamp,
                        LastResult = message.LastResult,
                        // ...
                    };

                    if (!string.IsNullOrEmpty(message.DataSetWriterId)) {
                        // Patch item state
                        if (!string.IsNullOrEmpty(message.PublishedVariableId)) {
                            await _datasets.UpdateDataSetVariableStateAsync(
                                message.DataSetWriterId, message.PublishedVariableId,
                                itemState, context);
                        }
                        else {
                            await _datasets.UpdateDataSetEventStateAsync(
                                message.DataSetWriterId, itemState, context);
                        }
                        break;
                    }

                    _logger.Warning("Monitored item event without dataset writer id");
                    break;

                default:
                    _logger.Error("Unknown event {eventId}", message.EventType);
                    break;
            }
        }

        private readonly IDataSetWriterStateUpdate _datasets;
        private readonly ILogger _logger;
    }
}
