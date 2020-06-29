// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Twin.Clients {
    using Microsoft.Azure.IIoT.Exceptions;
    using Microsoft.Azure.IIoT.OpcUa.Publisher;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Twin;
    using Microsoft.Azure.IIoT.OpcUa.Twin.Models;
    using Prometheus;
    using Serilog;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    /// <summary>
    /// Publisher twin adapter
    /// </summary>
    public sealed class PublisherAdapter : IPublishServices {

        /// <summary>
        /// Create client
        /// </summary>
        /// <param name="writers"></param>
        /// <param name="datasets"></param>
        /// <param name="logger"></param>
        public PublisherAdapter(IDataSetWriterRegistry writers,
            IDataSetBatchOperations datasets, ILogger logger) {
            _writers = writers ?? throw new ArgumentNullException(nameof(writers));
            _dataSets = datasets ?? throw new ArgumentNullException(nameof(datasets));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        public async Task<PublishStartResultModel> NodePublishStartAsync(
            string endpointId, PublishStartRequestModel request) {
            kNodePublishStart.Inc();
            if (string.IsNullOrEmpty(endpointId)) {
                throw new ArgumentNullException(nameof(endpointId));
            }
            if (request == null) {
                throw new ArgumentNullException(nameof(request));
            }
            if (request.Item == null) {
                throw new ArgumentNullException(nameof(request.Item));
            }
            if (string.IsNullOrEmpty(request.Item.NodeId)) {
                throw new ArgumentNullException(nameof(request.Item.NodeId));
            }
            var result = await _dataSets.AddVariablesToDefaultDataSetWriterAsync(endpointId,
                new DataSetAddVariableBatchRequestModel {
                    DataSetPublishingInterval = request.Item.PublishingInterval,
                    User = request.Header?.Elevation,
                    Variables = new List<DataSetAddVariableRequestModel> {
                        new DataSetAddVariableRequestModel {
                            PublishedVariableNodeId = request.Item.NodeId,
                            HeartbeatInterval = request.Item.HeartbeatInterval,
                            PublishedVariableDisplayName = request.Item.DisplayName,
                            SamplingInterval = request.Item.SamplingInterval,
                            QueueSize = 1
                        }
                    }
                });

            if (result.Results.Count != 1) {
                throw new InvalidOperationException(
                    "Unexpected response from dataset management service.");
            }
            return new PublishStartResultModel {
                ErrorInfo = result.Results[0].ErrorInfo
            };
        }

        /// <inheritdoc/>
        public async Task<PublishBulkResultModel> NodePublishBulkAsync(string endpointId,
            PublishBulkRequestModel request) {
            if (string.IsNullOrEmpty(endpointId)) {
                throw new ArgumentNullException(nameof(endpointId));
            }
            if (request == null) {
                throw new ArgumentNullException(nameof(request));
            }

            // This call will fail entirely or have removed at least parts of the variables.
            var remove = await _dataSets.RemoveVariablesFromDataSetWriterAsync(endpointId,
                new DataSetRemoveVariableBatchRequestModel {
                    Variables = request.NodesToRemove
                        .Select(n => new DataSetRemoveVariableRequestModel {
                            PublishedVariableNodeId = n,
                        })
                        .ToList()
                });
            var response = new PublishBulkResultModel {
                NodesToRemove = remove.Results.Select(r => r.ErrorInfo).ToList()
            };
            try {
                var publishingInterval = request.NodesToAdd?
                    .Min(p => p.PublishingInterval ?? TimeSpan.MaxValue);
                if (publishingInterval == TimeSpan.MaxValue) {
                    publishingInterval = null;
                }
                // This call will clean up in case of exception and thus have nothing added.
                var add = await _dataSets.AddVariablesToDefaultDataSetWriterAsync(endpointId,
                    new DataSetAddVariableBatchRequestModel {
                        DataSetPublishingInterval = publishingInterval,
                        User = request.Header?.Elevation,
                        Variables = request.NodesToAdd
                            .Select(n => new DataSetAddVariableRequestModel {
                                PublishedVariableNodeId = n.NodeId,
                                HeartbeatInterval = n.HeartbeatInterval,
                                PublishedVariableDisplayName = n.DisplayName,
                                SamplingInterval = n.SamplingInterval,
                                QueueSize = 1
                            })
                            .ToList()
                    });
                response.NodesToAdd = add.Results.Select(r => r.ErrorInfo).ToList();
            }
            catch (Exception ex) {
                _logger.Warning(ex, "Failed to add variables, returning partial results.");
                response.NodesToAdd = new List<Core.Models.ServiceResultModel> {
                    new Core.Models.ServiceResultModel {
                        ErrorMessage = ex.Message
                    }
                };
            }
            return response;
        }

        /// <inheritdoc/>
        public async Task<PublishStopResultModel> NodePublishStopAsync(
            string endpointId, PublishStopRequestModel request) {
            kNodePublishStop.Inc();
            if (string.IsNullOrEmpty(endpointId)) {
                throw new ArgumentNullException(nameof(endpointId));
            }
            if (request == null) {
                throw new ArgumentNullException(nameof(request));
            }
            if (string.IsNullOrEmpty(request.NodeId)) {
                throw new ArgumentNullException(nameof(request.NodeId));
            }

            var result = await _dataSets.RemoveVariablesFromDataSetWriterAsync(endpointId,
                new DataSetRemoveVariableBatchRequestModel {
                    Variables = new List<DataSetRemoveVariableRequestModel> {
                        new DataSetRemoveVariableRequestModel {
                            PublishedVariableNodeId = request.NodeId
                        }
                    }
                });

            if (result.Results.Count != 1) {
                throw new InvalidOperationException(
                    "Unexpected response from dataset management service.");
            }
            return new PublishStopResultModel {
                ErrorInfo = result.Results[0].ErrorInfo
            };
        }

        /// <inheritdoc/>
        public async Task<PublishedItemListResultModel> NodePublishListAsync(
            string endpointId, PublishedItemListRequestModel request) {
            if (string.IsNullOrEmpty(endpointId)) {
                throw new ArgumentNullException(nameof(endpointId));
            }
            if (request == null) {
                throw new ArgumentNullException(nameof(request));
            }
            try {
                var writer = await _writers.GetDataSetWriterAsync(endpointId);
                var dataset = writer.DataSet?.DataSetSource?.PublishedVariables?.PublishedData;
                return new PublishedItemListResultModel {
                    Items = dataset?.Select(d => new PublishedItemModel {
                        DisplayName = d.PublishedVariableDisplayName,
                        HeartbeatInterval = d.HeartbeatInterval,
                        NodeId = d.PublishedVariableNodeId,
                        SamplingInterval = d.SamplingInterval,
                        PublishingInterval =
                            writer.DataSet.DataSetSource.SubscriptionSettings?.PublishingInterval
                    }).ToList()
                };
            }
            catch (ResourceNotFoundException) {
                // Old behavior when none found return empty list
                return new PublishedItemListResultModel {
                    Items = new List<PublishedItemModel>()
                };
            }
        }

        private readonly IDataSetWriterRegistry _writers;
        private readonly IDataSetBatchOperations _dataSets;
        private readonly ILogger _logger;

        private static readonly Counter kNodePublishStart = Metrics
            .CreateCounter("iiot_publisher_node_publish_start", "calls to nodePublishStartAsync");
        private static readonly Counter kNodePublishStop = Metrics
            .CreateCounter("iiot_publisher_node_publish_stop", "calls to nodePublishStopAsync");
    }
}
