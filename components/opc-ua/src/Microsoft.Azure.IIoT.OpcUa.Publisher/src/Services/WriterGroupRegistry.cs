// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Services {
    using Microsoft.Azure.IIoT.OpcUa.Publisher;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Registry;
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;
    using Microsoft.Azure.IIoT.Serializers;
    using Microsoft.Azure.IIoT.Exceptions;
    using Microsoft.Azure.IIoT.Utils;
    using Serilog;
    using System.Threading.Tasks;
    using System.Threading;
    using System.Collections.Generic;
    using System.Linq;
    using System;

    /// <summary>
    /// PubSub configuration service manages the Publish configuration surface
    /// on top of the entity repositories and provides eventing support.
    /// </summary>
    public sealed class WriterGroupRegistry : IDataSetBatchOperations, IDataSetWriterRegistry,
        IWriterGroupRegistry {

        /// <summary>
        /// Create publisher registry service
        /// </summary>
        /// <param name="dataSets"></param>
        /// <param name="writers"></param>
        /// <param name="groups"></param>
        /// <param name="logger"></param>
        /// <param name="endpoints"></param>
        /// <param name="itemEvents"></param>
        /// <param name="writerEvents"></param>
        /// <param name="groupEvents"></param>
        public WriterGroupRegistry(IDataSetEntityRepository dataSets,
            IDataSetWriterRepository writers, IWriterGroupRepository groups,
            IEndpointRegistry endpoints,
            IPublisherEventBroker<IPublishedDataSetListener> itemEvents,
            IPublisherEventBroker<IDataSetWriterRegistryListener> writerEvents,
            IPublisherEventBroker<IWriterGroupRegistryListener> groupEvents,
            ILogger logger) {

            _dataSets = dataSets ?? throw new ArgumentNullException(nameof(dataSets));
            _writers = writers ?? throw new ArgumentNullException(nameof(writers));
            _groups = groups ?? throw new ArgumentNullException(nameof(groups));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _endpoints = endpoints ?? throw new ArgumentNullException(nameof(endpoints));

            _writerEvents = writerEvents ??
                throw new ArgumentNullException(nameof(writerEvents));
            _groupEvents = groupEvents ??
                throw new ArgumentNullException(nameof(groupEvents));
            _itemEvents = itemEvents ??
                throw new ArgumentNullException(nameof(itemEvents));
        }

        /// <inheritdoc/>
        public async Task<DataSetAddVariableBatchResultModel> AddVariablesToDataSetWriterAsync(
            string dataSetWriterId, DataSetAddVariableBatchRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (request?.Variables == null) {
                throw new ArgumentNullException(nameof(request));
            }
            if (request.Variables.Count == 0 || request.Variables.Count > kMaxBatchSize) {
                throw new ArgumentException(nameof(request.Variables),
                    "Number of variables in request is invalid.");
            }
            // Try to find writer
            var writer = await _writers.FindAsync(dataSetWriterId, ct);
            if (writer == null) {
                throw new ArgumentException(nameof(dataSetWriterId), "Writer not found");
            }
            var results = new List<DataSetAddVariableResultModel>();
            try {
                // Add variables - TODO consider adding a bulk database api.
                foreach (var variable in request.Variables) {
                    try {
                        var info = variable.AsDataSetVariable(context);
                        var result = await _dataSets.AddDataSetVariableAsync(
                            writer.DataSetWriterId, info);
                        results.Add(new DataSetAddVariableResultModel {
                            GenerationId = result.GenerationId,
                            Id = result.Id
                        });
                    }
                    catch (Exception ex) {
                        results.Add(new DataSetAddVariableResultModel {
                            ErrorInfo = new ServiceResultModel {
                                ErrorMessage = ex.Message
                                // ...
                            }
                        });
                    }
                }

                // If successful notify about dataset writer change
                await _writerEvents.NotifyAllAsync(
                    l => l.OnDataSetWriterUpdatedAsync(context, dataSetWriterId, writer));
                return new DataSetAddVariableBatchResultModel {
                    Results = results
                };
            }
            catch {
                // Undo add
                await Task.WhenAll(results.Select(item =>
                    Try.Async(() => _dataSets.DeleteDataSetVariableAsync(writer.DataSetWriterId,
                        item.Id, item.GenerationId))));
                throw;
            }
        }

        /// <inheritdoc/>
        public async Task<DataSetAddVariableBatchResultModel> AddVariablesToDefaultDataSetWriterAsync(
            string endpointId, DataSetAddVariableBatchRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            if (string.IsNullOrEmpty(endpointId)) {
                throw new ArgumentNullException(nameof(endpointId));
            }
            if (request?.Variables == null) {
                throw new ArgumentNullException(nameof(request));
            }
            if (request.Variables.Count == 0 || request.Variables.Count > kMaxBatchSize) {
                throw new ArgumentException(nameof(request.Variables),
                    "Number of variables in request is invalid.");
            }
            var results = new List<DataSetAddVariableResultModel>();
            try {
                // Add variables - TODO consider adding a bulk database api.
                foreach (var variable in request.Variables) {
                    try {
                        var info = variable.AsDataSetVariable(context);
                        var result = await _dataSets.AddDataSetVariableAsync(endpointId, info);
                        results.Add(new DataSetAddVariableResultModel {
                            GenerationId = result.GenerationId,
                            Id = result.Id
                        });
                    }
                    catch (Exception ex) {
                        results.Add(new DataSetAddVariableResultModel {
                            ErrorInfo = new ServiceResultModel {
                                ErrorMessage = ex.Message
                                // ...
                            }
                        });
                    }
                }
                var writer = await EnsureDefaultDataSetWriterExistsAsync(endpointId,
                    context, request.DataSetPublishingInterval, request.User, ct);
                // If successful notify about dataset writer change
                await _writerEvents.NotifyAllAsync(
                    l => l.OnDataSetWriterUpdatedAsync(context, endpointId, writer));
                return new DataSetAddVariableBatchResultModel {
                    Results = results
                };
            }
            catch {
                // Undo add
                await Task.WhenAll(results.Select(item =>
                    Try.Async(() => _dataSets.DeleteDataSetVariableAsync(endpointId,
                        item.Id, item.GenerationId))));
                throw;
            }
        }

        /// <inheritdoc/>
        public async Task<DataSetAddEventResultModel> AddEventDataSetAsync(
            string dataSetWriterId, DataSetAddEventRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (request == null) {
                throw new ArgumentNullException(nameof(request));
            }

            // This will fail if there is already a event dataset
            var result = await _dataSets.AddEventDataSetAsync(dataSetWriterId,
                request.AsEventDataSet(context));

            // If successful notify about dataset writer change
            await _itemEvents.NotifyAllAsync(
                l => l.OnPublishedDataSetEventsAddedAsync(context, dataSetWriterId, result));
            await _writerEvents.NotifyAllAsync(
                l => l.OnDataSetWriterUpdatedAsync(context, dataSetWriterId));

            return new DataSetAddEventResultModel {
                GenerationId = result.GenerationId
            };
        }

        /// <inheritdoc/>
        public async Task<DataSetAddVariableResultModel> AddDataSetVariableAsync(
            string dataSetWriterId, DataSetAddVariableRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (request == null) {
                throw new ArgumentNullException(nameof(request));
            }

            // This will succeed
            var result = await _dataSets.AddDataSetVariableAsync(dataSetWriterId,
                request.AsDataSetVariable(context));

            // If successful notify about dataset writer change
            await _itemEvents.NotifyAllAsync(
                l => l.OnPublishedDataSetVariableAddedAsync(context, dataSetWriterId, result));
            await _writerEvents.NotifyAllAsync(
                l => l.OnDataSetWriterUpdatedAsync(context, dataSetWriterId));

            return new DataSetAddVariableResultModel {
                GenerationId = result.GenerationId,
                Id = result.Id
            };
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterAddResultModel> AddDataSetWriterAsync(
            DataSetWriterAddRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            if (request == null) {
                throw new ArgumentNullException(nameof(request));
            }
            if (string.IsNullOrEmpty(request.EndpointId)) {
                throw new ArgumentNullException(nameof(request.EndpointId));
            }

            // Find the specified endpoint and fail if not exist
            var endpoint = await Try.Async(() => _endpoints.GetEndpointAsync(
                request.EndpointId, true, ct));
            ct.ThrowIfCancellationRequested();
            if (endpoint == null) {
                throw new ArgumentException(nameof(request.EndpointId),
                    "Endpoint not found");
            }

            // Check writer group in same site
            if (!string.IsNullOrEmpty(request.WriterGroupId)) {
                var group = await _groups.FindAsync(request.WriterGroupId);
                if (group == null) {
                    throw new ArgumentException(nameof(request.WriterGroupId),
                        "Dataset writer group not found.");
                }
                if (group.SiteId != endpoint.Registration.SiteId) {
                    throw new ArgumentException(nameof(request.WriterGroupId),
                        "Dataset writer group not in same site as endpoint");
                }
            }
            else {
                // Use default writer group for site
                request.WriterGroupId = endpoint.Registration.SiteId;
            }

            var result = await _writers.AddAsync(request.AsDataSetWriterInfo(context));

            // If successful notify about dataset writer creation
            await _writerEvents.NotifyAllAsync(
                l => l.OnDataSetWriterAddedAsync(context, result));

            // Make sure the default group is created if it does not exist yet
            if (request.WriterGroupId == endpoint.Registration.SiteId) {
                await Try.Async(() => EnsureDefaultWriterGroupExistsAsync(
                     endpoint.Registration.SiteId, context, ct));
            }

            return new DataSetWriterAddResultModel {
                GenerationId = result.GenerationId,
                DataSetWriterId = result.DataSetWriterId
            };
        }

        /// <inheritdoc/>
        public async Task<WriterGroupAddResultModel> AddWriterGroupAsync(
            WriterGroupAddRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            if (request == null) {
                throw new ArgumentNullException(nameof(request));
            }
            if (string.IsNullOrEmpty(request.SiteId)) {
                throw new ArgumentNullException(nameof(request.SiteId));
            }
            var result = await _groups.AddAsync(request.AsWriterGroupInfo(context));

            // If successful notify about group creation
            await _groupEvents.NotifyAllAsync(
                l => l.OnWriterGroupAddedAsync(context, result));

            return new WriterGroupAddResultModel {
                GenerationId = result.GenerationId,
                WriterGroupId = result.WriterGroupId
            };
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterModel> GetDataSetWriterAsync(
            string dataSetWriterId, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            var result = await _writers.FindAsync(dataSetWriterId, ct);
            if (result == null) {
                throw new ResourceNotFoundException("Dataset Writer not found");
            }
            return await GetDataSetWriterAsync(result, ct);
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetEventsModel> GetEventDataSetAsync(
            string dataSetWriterId, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            var result = await _dataSets.FindEventDataSetAsync(dataSetWriterId, ct);
            if (result == null) {
                throw new ResourceNotFoundException("Event dataset not found");
            }
            return result;
        }

        /// <inheritdoc/>
        public async Task<WriterGroupModel> GetWriterGroupAsync(
            string writerGroupId, CancellationToken ct) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            var result = await _groups.FindAsync(writerGroupId, ct);
            if (result == null) {
                throw new ResourceNotFoundException("Writer group not found");
            }
            // Collect writers
            string continuationToken = null;
            var writers = new List<DataSetWriterModel>();
            do {
                // Get writers one by one
                var results = await _writers.QueryAsync(new DataSetWriterInfoQueryModel {
                    WriterGroupId = writerGroupId
                }, continuationToken, null, ct);
                continuationToken = results.ContinuationToken;
                foreach (var writer in results.DataSetWriters) {
                    var expanded = await GetDataSetWriterAsync(writer, ct);
                    writers.Add(expanded);
                }
            }
            while (continuationToken != null);
            return result.AsWriterGroup(writers);
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterInfoListModel> ListDataSetWritersAsync(
            string continuation, int? pageSize, CancellationToken ct) {
            return await _writers.QueryAsync(null, continuation, pageSize, ct);
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetVariableListModel> ListDataSetVariablesAsync(
            string dataSetWriterId, string continuation, int? pageSize, CancellationToken ct) {
            return await _dataSets.QueryDataSetVariablesAsync(dataSetWriterId,
                null, continuation, pageSize, ct);
        }

        /// <inheritdoc/>
        public async Task<WriterGroupInfoListModel> ListWriterGroupsAsync(
            string continuation, int? pageSize, CancellationToken ct) {
            return await _groups.QueryAsync(null, continuation, pageSize, ct);
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetVariableListModel> QueryDataSetVariablesAsync(
            string dataSetWriterId, PublishedDataSetVariableQueryModel query, int? pageSize,
            CancellationToken ct) {
            return await _dataSets.QueryDataSetVariablesAsync(dataSetWriterId,
                query, null, pageSize, ct);
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterInfoListModel> QueryDataSetWritersAsync(
            DataSetWriterInfoQueryModel query, int? pageSize, CancellationToken ct) {
            return await _writers.QueryAsync(query, null, pageSize, ct);
        }

        /// <inheritdoc/>
        public async Task<WriterGroupInfoListModel> QueryWriterGroupsAsync(
            WriterGroupInfoQueryModel query, int? pageSize, CancellationToken ct) {
            return await _groups.QueryAsync(query, null, pageSize, ct);
        }

        /// <inheritdoc/>
        public async Task UpdateEventDataSetAsync(string dataSetWriterId,
            DataSetUpdateEventRequestModel request, PublisherOperationContextModel context,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (request == null) {
                throw new ArgumentNullException(nameof(request));
            }
            var updated = false;
            var result = await _dataSets.UpdateEventDataSetAsync(dataSetWriterId, existing => {
                if (request.GenerationId != null &&
                    request.GenerationId != existing.GenerationId) {
                    throw new ResourceOutOfDateException("Generation does not match.");
                }
                if (request.DiscardNew != null) {
                    existing.DiscardNew = request.DiscardNew == false ?
                        null : request.DiscardNew;
                    updated = true;
                }
                if (request.MonitoringMode != null) {
                    existing.MonitoringMode = request.MonitoringMode == 0 ?
                        null : request.MonitoringMode;
                    updated = true;
                }
                if (request.QueueSize != null) {
                    existing.QueueSize = request.QueueSize == 0 ?
                        null : request.QueueSize;
                    updated = true;
                }
                if (request.TriggerId != null) {
                    existing.TriggerId = string.IsNullOrEmpty(request.TriggerId) ?
                        null : request.TriggerId;
                    updated = true;
                }
                if (request.SelectedFields != null) {
                    existing.SelectedFields = request.SelectedFields.Count == 0 ?
                        null : request.SelectedFields;
                    updated = true;
                }
                if (request.Filter?.Elements != null) {
                    existing.Filter = request.Filter.Elements.Count == 0 ?
                        null : request.Filter;
                    updated = true;
                }
                return Task.FromResult(updated);
            }, ct);
            if (updated) {
                // If updated notify about dataset writer change
                await _itemEvents.NotifyAllAsync(
                    l => l.OnPublishedDataSetEventsUpdatedAsync(context, dataSetWriterId, result));
                await _writerEvents.NotifyAllAsync(
                    l => l.OnDataSetWriterUpdatedAsync(context, dataSetWriterId));
            }
        }

        /// <inheritdoc/>
        public async Task UpdateDataSetVariableAsync(string dataSetWriterId,
            string variableId, DataSetUpdateVariableRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (string.IsNullOrEmpty(variableId)) {
                throw new ArgumentNullException(nameof(variableId));
            }
            if (request == null) {
                throw new ArgumentNullException(nameof(request));
            }
            var updated = false;
            var result = await _dataSets.UpdateDataSetVariableAsync(dataSetWriterId,
                variableId, existing => {
                if (request.GenerationId != null &&
                    request.GenerationId != existing.GenerationId) {
                    throw new ResourceOutOfDateException("Generation does not match.");
                }
                if (request.DiscardNew != null) {
                    existing.DiscardNew = request.DiscardNew == false ?
                        null : request.DiscardNew;
                    updated = true;
                }
                if (request.DeadbandType != null) {
                    existing.DeadbandType = request.DeadbandType == 0 ?
                        null : request.DeadbandType;
                    updated = true;
                }
                if (request.DataChangeFilter != null) {
                    existing.DataChangeFilter = request.DataChangeFilter == 0 ?
                        null : request.DataChangeFilter;
                    updated = true;
                }
                if (request.SubstituteValue != null) {
                    existing.SubstituteValue = request.SubstituteValue.IsNull() ?
                        null : request.SubstituteValue;
                    updated = true;
                }
                if (request.QueueSize != null) {
                    existing.QueueSize = request.QueueSize == 0 ?
                        null : request.QueueSize;
                    updated = true;
                }
                if (request.MonitoringMode != null) {
                    existing.MonitoringMode = request.MonitoringMode == 0 ?
                        null : request.MonitoringMode;
                    updated = true;
                }
                if (request.SamplingInterval != null) {
                    existing.SamplingInterval = request.SamplingInterval <= TimeSpan.Zero ?
                        null : request.SamplingInterval;
                    updated = true;
                }
                if (request.HeartbeatInterval != null) {
                    existing.HeartbeatInterval = request.HeartbeatInterval <= TimeSpan.Zero ?
                        null : request.HeartbeatInterval;
                    updated = true;
                }
                if (request.TriggerId != null) {
                    existing.TriggerId = string.IsNullOrEmpty(request.TriggerId) ?
                        null : request.TriggerId;
                    updated = true;
                }
                if (request.PublishedVariableDisplayName != null) {
                    existing.PublishedVariableDisplayName =
                    string.IsNullOrEmpty(request.PublishedVariableDisplayName) ?
                        null : request.PublishedVariableDisplayName;
                    updated = true;
                }
                return Task.FromResult(updated);
            }, ct);
            if (updated) {
                // If updated notify about dataset writer change
                await _itemEvents.NotifyAllAsync(
                    l => l.OnPublishedDataSetVariableUpdatedAsync(context, dataSetWriterId, result));
                await _writerEvents.NotifyAllAsync(
                    l => l.OnDataSetWriterUpdatedAsync(context, dataSetWriterId));
            }
        }

        /// <inheritdoc/>
        public async Task UpdateDataSetWriterAsync(string dataSetWriterId,
            DataSetWriterUpdateRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (request == null) {
                throw new ArgumentNullException(nameof(request));
            }
            var updated = false;
            var writer = await _writers.UpdateAsync(dataSetWriterId, existing => {
                if (request.GenerationId != null &&
                    request.GenerationId != existing.GenerationId) {
                    throw new ResourceOutOfDateException("Generation does not match.");
                }
                if (request.WriterGroupId != null) {
                    existing.WriterGroupId = string.IsNullOrEmpty(request.WriterGroupId) ?
                        null : request.WriterGroupId;
                    updated = true;
                }
                if (request.DataSetFieldContentMask != null) {
                    existing.DataSetFieldContentMask = request.DataSetFieldContentMask == 0 ?
                        null : request.DataSetFieldContentMask;
                    updated = true;
                }
                if (request.KeyFrameCount != null) {
                    existing.KeyFrameCount = request.KeyFrameCount == 0 ?
                        null : request.KeyFrameCount;
                    updated = true;
                }
                if (request.KeyFrameInterval != null) {
                    existing.KeyFrameInterval = request.KeyFrameInterval <= TimeSpan.Zero ?
                        null : request.KeyFrameInterval;
                    updated = true;
                }

                if (existing.DataSet == null) {
                    existing.DataSet = new PublishedDataSetSourceInfoModel();
                    updated = true;
                }

                if (request.ExtensionFields != null) {
                    existing.DataSet.ExtensionFields = request.ExtensionFields.Count == 0 ?
                        null : request.ExtensionFields;
                    updated = true;
                }
                if (request.DataSetName != null) {
                    existing.DataSet.Name = string.IsNullOrEmpty(request.DataSetName) ?
                        null : request.DataSetName;
                    updated = true;
                }

                if (request.SubscriptionSettings != null) {
                    if (existing.DataSet.SubscriptionSettings == null) {
                        existing.DataSet.SubscriptionSettings = new PublishedDataSetSourceSettingsModel();
                        updated = true;
                    }
                    if (request.SubscriptionSettings.MaxKeepAliveCount != null) {
                        existing.DataSet.SubscriptionSettings.MaxKeepAliveCount =
                            request.SubscriptionSettings.MaxKeepAliveCount == 0 ?
                            null : request.SubscriptionSettings.MaxKeepAliveCount;
                    }
                    if (request.SubscriptionSettings.MaxNotificationsPerPublish != null) {
                        existing.DataSet.SubscriptionSettings.MaxNotificationsPerPublish =
                            request.SubscriptionSettings.MaxNotificationsPerPublish == 0 ?
                            null : request.SubscriptionSettings.MaxNotificationsPerPublish;
                    }
                    if (request.SubscriptionSettings.LifeTimeCount != null) {
                        existing.DataSet.SubscriptionSettings.LifeTimeCount =
                            request.SubscriptionSettings.LifeTimeCount == 0 ?
                            null : request.SubscriptionSettings.LifeTimeCount;
                    }
                    if (request.SubscriptionSettings.Priority != null) {
                        existing.DataSet.SubscriptionSettings.Priority =
                            request.SubscriptionSettings.Priority == 0 ?
                            null : request.SubscriptionSettings.Priority;
                    }
                    if (request.SubscriptionSettings.PublishingInterval != null) {
                        existing.DataSet.SubscriptionSettings.PublishingInterval =
                            request.SubscriptionSettings.PublishingInterval <= TimeSpan.Zero ?
                            null : request.SubscriptionSettings.PublishingInterval;
                    }
                    if (request.SubscriptionSettings.ResolveDisplayName != null) {
                        existing.DataSet.SubscriptionSettings.ResolveDisplayName =
                            request.SubscriptionSettings.ResolveDisplayName == false ?
                            null : request.SubscriptionSettings.ResolveDisplayName;
                    }
                    updated = true;
                }
                if (request.MessageSettings != null) {
                    if (existing.MessageSettings == null) {
                        existing.MessageSettings = new DataSetWriterMessageSettingsModel();
                        updated = true;
                    }
                    if (request.MessageSettings.ConfiguredSize != null) {
                        existing.MessageSettings.ConfiguredSize =
                            request.MessageSettings.ConfiguredSize == 0 ?
                            null : request.MessageSettings.ConfiguredSize;
                    }
                    if (request.MessageSettings.DataSetMessageContentMask != null) {
                        existing.MessageSettings.DataSetMessageContentMask =
                            request.MessageSettings.DataSetMessageContentMask == 0 ?
                            null : request.MessageSettings.DataSetMessageContentMask;
                    }
                    if (request.MessageSettings.DataSetOffset != null) {
                        existing.MessageSettings.DataSetOffset =
                            request.MessageSettings.DataSetOffset == 0 ?
                            null : request.MessageSettings.DataSetOffset;
                    }
                    if (request.MessageSettings.NetworkMessageNumber != null) {
                        existing.MessageSettings.NetworkMessageNumber =
                            request.MessageSettings.NetworkMessageNumber == 0 ?
                            null : request.MessageSettings.NetworkMessageNumber;
                    }
                    updated = true;
                }
                if (request.User != null) {
                    existing.DataSet.User = request.User.Type == null ?
                        null : request.User;
                    updated = true;
                }
                return Task.FromResult(updated);
            }, ct);
            if (updated) {
                // If updated notify about dataset writer change
                await _writerEvents.NotifyAllAsync(
                    l => l.OnDataSetWriterUpdatedAsync(context, dataSetWriterId, writer));
            }
        }

        /// <inheritdoc/>
        public async Task UpdateWriterGroupAsync(string writerGroupId,
            WriterGroupUpdateRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            if (request == null) {
                throw new ArgumentNullException(nameof(request));
            }
            var updated = false;
            var group = await _groups.UpdateAsync(writerGroupId, existing => {
                if (request.GenerationId != null &&
                    request.GenerationId != existing.GenerationId) {
                    throw new ResourceOutOfDateException("Generation does not match.");
                }
                if (request.HeaderLayoutUri != null) {
                    existing.HeaderLayoutUri = string.IsNullOrEmpty(request.HeaderLayoutUri) ?
                        null : request.HeaderLayoutUri;
                    updated = true;
                }
                if (request.MessageType != null) {
                    existing.MessageType = request.MessageType == 0 ?
                        null : request.MessageType;
                    updated = true;
                }
                if (request.BatchSize != null) {
                    existing.BatchSize = request.BatchSize == 0 ?
                        null : request.BatchSize;
                    updated = true;
                }
                if (request.Priority != null) {
                    existing.Priority = request.Priority == 0 ?
                        null : request.Priority;
                    updated = true;
                }
                if (request.KeepAliveTime != null) {
                    existing.KeepAliveTime = request.KeepAliveTime <= TimeSpan.Zero ?
                        null : request.KeepAliveTime;
                    updated = true;
                }
                if (request.PublishingInterval != null) {
                    existing.PublishingInterval = request.PublishingInterval <= TimeSpan.Zero ?
                        null : request.PublishingInterval;
                    updated = true;
                }
                if (request.LocaleIds != null) {
                    existing.LocaleIds = request.LocaleIds.Count == 0 ?
                        null : request.LocaleIds;
                    updated = true;
                }
                if (request.Name != null) {
                    existing.Name = string.IsNullOrEmpty(request.Name) ?
                        null : request.Name;
                    updated = true;
                }
                if (request.MessageSettings != null) {
                    if (existing.MessageSettings == null) {
                        existing.MessageSettings = new WriterGroupMessageSettingsModel();
                        updated = true;
                    }
                    if (request.MessageSettings.NetworkMessageContentMask != null) {
                        existing.MessageSettings.NetworkMessageContentMask =
                            request.MessageSettings.NetworkMessageContentMask == 0 ?
                            null : request.MessageSettings.NetworkMessageContentMask;
                    }
                    if (request.MessageSettings.DataSetOrdering != null) {
                        existing.MessageSettings.DataSetOrdering =
                            request.MessageSettings.DataSetOrdering == 0 ?
                            null : request.MessageSettings.DataSetOrdering;
                    }
                    if (request.MessageSettings.GroupVersion != null) {
                        existing.MessageSettings.GroupVersion =
                            request.MessageSettings.GroupVersion == 0 ?
                            null : request.MessageSettings.GroupVersion;
                    }
                    if (request.MessageSettings.SamplingOffset != null) {
                        existing.MessageSettings.SamplingOffset =
                            request.MessageSettings.SamplingOffset == 0 ?
                            null : request.MessageSettings.SamplingOffset;
                    }
                    if (request.MessageSettings.PublishingOffset != null) {
                        existing.MessageSettings.PublishingOffset =
                            request.MessageSettings.PublishingOffset.Count == 0 ?
                            null : request.MessageSettings.PublishingOffset;
                    }
                    updated = true;
                }
                return Task.FromResult(updated);
            }, ct);
            if (updated) {
                // If updated notify about group change
                await _groupEvents.NotifyAllAsync(
                    l => l.OnWriterGroupUpdatedAsync(context, group));
            }
        }

        /// <inheritdoc/>
        public async Task<DataSetRemoveVariableBatchResultModel> RemoveVariablesFromDataSetWriterAsync(
            string dataSetWriterId, DataSetRemoveVariableBatchRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (request?.Variables == null) {
                throw new ArgumentNullException(nameof(request));
            }
            if (request.Variables.Count == 0 || request.Variables.Count > kMaxBatchSize) {
                throw new ArgumentException(nameof(request.Variables),
                    "Number of variables in request is invalid.");
            }

            // Todo - should we go case insensitive?
            var set = request.Variables.ToDictionary(v => v.PublishedVariableNodeId, v => 0);
            string continuation = null;
            var updated = false;
            do {
                var result = await _dataSets.QueryDataSetVariablesAsync(dataSetWriterId,
                    null, continuation, null, ct);
                continuation = result.ContinuationToken;
                if (result.Variables == null) {
                    continue;
                }
                foreach (var variable in result.Variables) {
                    if (variable != null && set.ContainsKey(variable.PublishedVariableNodeId)) {
                        await _dataSets.DeleteDataSetVariableAsync(dataSetWriterId,
                            variable.Id, variable.GenerationId, ct);
                        set[variable.PublishedVariableNodeId]++;
                        updated = true;
                    }
                }
            }
            while (continuation != null);
            if (updated) {
                // If successful update notify about dataset writer change
                await _writerEvents.NotifyAllAsync(
                    l => l.OnDataSetWriterUpdatedAsync(context, dataSetWriterId));
            }

            return new DataSetRemoveVariableBatchResultModel {
                Results = request.Variables.Select(v => set[v.PublishedVariableNodeId] == 0 ?
                null : new DataSetRemoveVariableResultModel {
                    ErrorInfo = new ServiceResultModel { ErrorMessage = "Item not found" }
                }).ToList()
            };
        }

        /// <inheritdoc/>
        public async Task RemoveDataSetVariableAsync(
            string dataSetWriterId, string variableId, string generationId,
            PublisherOperationContextModel context, CancellationToken ct) {
            if (string.IsNullOrEmpty(variableId)) {
                throw new ArgumentNullException(nameof(variableId));
            }
            await _dataSets.DeleteDataSetVariableAsync(dataSetWriterId,
                variableId, generationId, ct);

            await _itemEvents.NotifyAllAsync(
                l => l.OnPublishedDataSetVariableRemovedAsync(context, dataSetWriterId, variableId));
            await _writerEvents.NotifyAllAsync(
                l => l.OnDataSetWriterUpdatedAsync(context, dataSetWriterId));
        }

        /// <inheritdoc/>
        public async Task RemoveEventDataSetAsync(string dataSetWriterId,
            string generationId, PublisherOperationContextModel context,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            await _dataSets.DeleteEventDataSetAsync(dataSetWriterId, generationId, ct);

            await _itemEvents.NotifyAllAsync(
                l => l.OnPublishedDataSetEventsRemovedAsync(context, dataSetWriterId));
            await _writerEvents.NotifyAllAsync(
                l => l.OnDataSetWriterUpdatedAsync(context, dataSetWriterId));
        }

        /// <inheritdoc/>
        public async Task RemoveDataSetWriterAsync(string dataSetWriterId,
            string generationId, PublisherOperationContextModel context,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            var writer = await _writers.DeleteAsync(dataSetWriterId, async existing => {
                if (generationId != existing.GenerationId) {
                    throw new ResourceOutOfDateException("Generation does not match.");
                }
                // Force delete all dataset entities
                await Try.Async(() => _dataSets.DeleteDataSetAsync(dataSetWriterId));
                return true;
            }, ct);
            await _writerEvents.NotifyAllAsync(
                l => l.OnDataSetWriterRemovedAsync(context, writer));
        }

        /// <inheritdoc/>
        public async Task RemoveWriterGroupAsync(string writerGroupId,
            string generationId, PublisherOperationContextModel context,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            // If there are writers in the group we fail removal
            var result = await _writers.QueryAsync(new DataSetWriterInfoQueryModel {
                WriterGroupId = writerGroupId
            }, null, 1, ct);
            if (result.DataSetWriters.Any()) {
                throw new ResourceInvalidStateException(
                    "Remove all writers from the group before you remove the group.");
            }
            await _groups.DeleteAsync(writerGroupId, generationId, ct);
            await _groupEvents.NotifyAllAsync(
                l => l.OnWriterGroupRemovedAsync(context, writerGroupId));
        }

        /// <summary>
        /// Collect all bits to create a data set writer
        /// </summary>
        /// <param name="writerInfo"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        private async Task<DataSetWriterModel> GetDataSetWriterAsync(
            DataSetWriterInfoModel writerInfo, CancellationToken ct) {
            var endpoint = string.IsNullOrEmpty(writerInfo.DataSet.EndpointId) ? null :
                await _endpoints.GetEndpointAsync(writerInfo.DataSet.EndpointId, false, ct);
            var connection = endpoint.Registration?.Endpoint == null ? null :
                new ConnectionModel {
                    Endpoint = endpoint.Registration.Endpoint.Clone(),
                    User = writerInfo.DataSet?.User.Clone()
                };
            // Find event
            var events = await _dataSets.FindEventDataSetAsync(writerInfo.DataSetWriterId, ct);
            if (events != null) {
                return writerInfo.AsDataSetWriter(connection, null, events);
            }
            // Get variables
            var publishedData = new List<PublishedDataSetVariableModel>();
            string continuation = null;
            do {
                var result = await _dataSets.QueryDataSetVariablesAsync(
                    writerInfo.DataSetWriterId, null, continuation, null, ct);
                continuation = result.ContinuationToken;
                if (result.Variables != null) {
                    publishedData.AddRange(result.Variables.Where(item => item != null));
                }
            }
            while (continuation != null);
            return writerInfo.AsDataSetWriter(connection, new PublishedDataItemsModel {
                PublishedData = publishedData
            }, null);
        }

        /// <summary>
        /// Ensure default writer group exists
        /// </summary>
        /// <param name="siteId"></param>
        /// <param name="context"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        private async Task<WriterGroupInfoModel> EnsureDefaultWriterGroupExistsAsync(
            string siteId, PublisherOperationContextModel context, CancellationToken ct) {
            var group = await _groups.AddOrUpdateAsync(siteId,
                group => {
                    if (group != null) {
                        group = null; // No need to add
                    }
                    else {
                        group = new WriterGroupInfoModel {
                            Name = $"Default Writer Group ({siteId})",
                            WriterGroupId = siteId,
                            SiteId = siteId,
                            Created = context,
                            Updated = context,
                            MessageType = NetworkMessageType.Uadp
                        };
                    }
                    return Task.FromResult(group);
                }, ct);
            if (group != null) {
                // Group added
                await _groupEvents.NotifyAllAsync(
                    l => l.OnWriterGroupAddedAsync(context, group));
            }
            return group;
        }

        /// <summary>
        /// Ensure default writer for endpoint exists
        /// </summary>
        /// <param name="endpointId"></param>
        /// <param name="context"></param>
        /// <param name="publishingInterval"></param>
        /// <param name="user"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        private async Task<DataSetWriterInfoModel> EnsureDefaultDataSetWriterExistsAsync(
            string endpointId, PublisherOperationContextModel context,
            TimeSpan? publishingInterval, CredentialModel user, CancellationToken ct) {
            // Find the specified endpoint and fail if not exist
            var endpoint = await Try.Async(() => _endpoints.GetEndpointAsync(
                endpointId, false, ct));
            ct.ThrowIfCancellationRequested();
            if (endpoint == null) {
                throw new ArgumentException(nameof(endpointId), "Endpoint not found.");
            }
            if (string.IsNullOrEmpty(endpoint.Registration?.SiteId)) {
                throw new ResourceInvalidStateException("Endpoint has not site id.");
            }

            var added = false;
            var writer = await _writers.AddOrUpdateAsync(endpointId,
                writer => {
                    if (writer != null) {
                        if (publishingInterval == null && user == null) {
                            writer = null;
                        }
                        else {
                            if (writer.DataSet == null) {
                                writer.DataSet = new PublishedDataSetSourceInfoModel();
                            }
                            if (user != null) {
                                writer.DataSet.User = user;
                            }
                            if (publishingInterval != null) {
                                if (writer.DataSet.SubscriptionSettings == null) {
                                    writer.DataSet.SubscriptionSettings =
                                        new PublishedDataSetSourceSettingsModel();
                                }
                                writer.DataSet.SubscriptionSettings.PublishingInterval =
                                    publishingInterval;
                            }
                            writer.WriterGroupId = endpoint.Registration.SiteId;
                        }
                    }
                    else {
                        added = true;
                        writer = new DataSetWriterInfoModel {
                            DataSet = new PublishedDataSetSourceInfoModel {
                                Name = $"Default Writer ({endpointId})",
                                EndpointId = endpointId,
                                User = user,
                                SubscriptionSettings = publishingInterval == null ?
                                    null : new PublishedDataSetSourceSettingsModel {
                                        PublishingInterval = publishingInterval
                                    }
                            },
                            DataSetWriterId = endpointId,
                            WriterGroupId = endpoint.Registration.SiteId,
                            Created = context,
                            Updated = context
                        };
                    }
                    return Task.FromResult(writer);
                }, ct);

            var group = await EnsureDefaultWriterGroupExistsAsync(
                endpoint.Registration.SiteId, context, ct);
            if (writer != null) {
                if (added) {
                    _logger.Information("Added default group for {endpointId}", endpointId);
                    // Writer added
                    await _writerEvents.NotifyAllAsync(
                        l => l.OnDataSetWriterAddedAsync(context, writer));
                }
                if (group != null) {
                    // and thus group changed
                    await _groupEvents.NotifyAllAsync(
                        l => l.OnWriterGroupUpdatedAsync(context, group));
                }
            }
            return writer;
        }

        private const int kMaxBatchSize = 1000;
        private readonly ILogger _logger;
        private readonly IDataSetEntityRepository _dataSets;
        private readonly IDataSetWriterRepository _writers;
        private readonly IWriterGroupRepository _groups;
        private readonly IEndpointRegistry _endpoints;

        private readonly IPublisherEventBroker<IPublishedDataSetListener> _itemEvents;
        private readonly IPublisherEventBroker<IDataSetWriterRegistryListener> _writerEvents;
        private readonly IPublisherEventBroker<IWriterGroupRegistryListener> _groupEvents;
    }
}
