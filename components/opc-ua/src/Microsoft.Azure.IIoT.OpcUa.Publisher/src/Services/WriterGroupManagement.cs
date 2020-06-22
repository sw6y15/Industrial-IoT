// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Services {
    using Microsoft.Azure.IIoT.OpcUa.Publisher;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using System.Threading.Tasks;
    using System.Threading;
    using System;

    /// <summary>
    /// Manage writer group and contained writer state
    /// </summary>
    public sealed class WriterGroupManagement : IDataSetWriterStateUpdate,
        IWriterGroupStateUpdate {

        /// <summary>
        /// Create publisher registry service
        /// </summary>
        /// <param name="dataSets"></param>
        /// <param name="writers"></param>
        /// <param name="groups"></param>
        /// <param name="itemEvents"></param>
        /// <param name="writerEvents"></param>
        /// <param name="groupEvents"></param>
        public WriterGroupManagement(IDataSetEntityRepository dataSets,
            IDataSetWriterRepository writers, IWriterGroupRepository groups,
            IPublisherEventBroker<IPublishedDataSetListener> itemEvents,
            IPublisherEventBroker<IDataSetWriterRegistryListener> writerEvents,
            IPublisherEventBroker<IWriterGroupRegistryListener> groupEvents) {

            _dataSets = dataSets ?? throw new ArgumentNullException(nameof(dataSets));
            _writers = writers ?? throw new ArgumentNullException(nameof(writers));
            _groups = groups ?? throw new ArgumentNullException(nameof(groups));

            _writerEvents = writerEvents ??
                throw new ArgumentNullException(nameof(writerEvents));
            _groupEvents = groupEvents ??
                throw new ArgumentNullException(nameof(groupEvents));
            _itemEvents = itemEvents ??
                throw new ArgumentNullException(nameof(itemEvents));
        }

        /// <inheritdoc/>
        public async Task UpdateDataSetEventStateAsync(string dataSetWriterId,
            PublishedDataSetItemStateModel state, PublisherOperationContextModel context,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (state == null) {
                throw new ArgumentNullException(nameof(state));
            }
            var updated = false;
            var lastResultChange = state.LastResultChange ?? context?.Time ?? DateTime.UtcNow;
            var result = await _dataSets.UpdateEventDataSetAsync(dataSetWriterId, existing => {
                if (existing?.State != null) {
                    updated = true;
                    existing.State.LastResult = state.LastResult;
                    existing.State.LastResultChange = lastResultChange;
                    existing.State.ServerId = state.ServerId;
                    existing.State.ClientId = state.ClientId;
                }
                else if (state.LastResult != null) {
                    updated = true;
                    existing.State = new PublishedDataSetItemStateModel {
                        LastResult = state.LastResult,
                        LastResultChange = lastResultChange,
                        ClientId = state.ClientId,
                        ServerId = state.ServerId,
                    };
                }
                return Task.FromResult(updated);
            }, ct);
            if (updated) {
                // If updated notify about dataset writer change
                await _itemEvents.NotifyAllAsync(
                    l => l.OnPublishedDataSetEventsStateChangeAsync(context, dataSetWriterId, result));
                await _writerEvents.NotifyAllAsync(
                    l => l.OnDataSetWriterStateChangeAsync(context, dataSetWriterId, null));
            }
        }

        /// <inheritdoc/>
        public async Task UpdateDataSetVariableStateAsync(string dataSetWriterId,
            string variableId, PublishedDataSetItemStateModel state,
            PublisherOperationContextModel context, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (string.IsNullOrEmpty(variableId)) {
                throw new ArgumentNullException(nameof(variableId));
            }
            if (state == null) {
                throw new ArgumentNullException(nameof(state));
            }
            var lastResultChange = state.LastResultChange ?? context?.Time ?? DateTime.UtcNow;
            var updated = false;
            var result = await _dataSets.UpdateDataSetVariableAsync(dataSetWriterId,
                variableId, existing => {
                    if (existing?.State != null) {
                        updated = true;
                        existing.State.LastResult = state.LastResult;
                        existing.State.LastResultChange = lastResultChange;
                        existing.State.ServerId = state.ServerId;
                        existing.State.ClientId = state.ClientId;
                    }
                    else if (state.LastResult != null) {
                        updated = true;
                        existing.State = new PublishedDataSetItemStateModel {
                            LastResult = state.LastResult,
                            LastResultChange = lastResultChange,
                            ClientId = state.ClientId,
                            ServerId = state.ServerId,
                        };
                    }
                    return Task.FromResult(updated);
            }, ct);
            if (updated) {
                // If updated notify about dataset writer change
                await _itemEvents.NotifyAllAsync(
                    l => l.OnPublishedDataSetVariableStateChangeAsync(context, dataSetWriterId, result));
                await _writerEvents.NotifyAllAsync(
                    l => l.OnDataSetWriterStateChangeAsync(context, dataSetWriterId, null));
            }
        }


        /// <inheritdoc/>
        public async Task UpdateDataSetWriterStateAsync(string dataSetWriterId,
            PublishedDataSetSourceStateModel state, PublisherOperationContextModel context,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (state == null) {
                throw new ArgumentNullException(nameof(state));
            }
            var updated = false;
            var lastResultChange = state.LastResultChange ?? context?.Time ?? DateTime.UtcNow;
            var writer = await _writers.UpdateAsync(dataSetWriterId, existing => {
                if (existing?.DataSet?.State != null) {
                    updated = true;
                    existing.DataSet.State.LastResult = state.LastResult;
                    existing.DataSet.State.LastResultChange = lastResultChange;
                }
                else if (state.LastResult != null) {
                    updated = true;
                    if (existing.DataSet == null) {
                        existing.DataSet = new PublishedDataSetSourceInfoModel();
                    }
                    existing.DataSet.State = new PublishedDataSetSourceStateModel {
                        LastResult = state.LastResult,
                        LastResultChange = lastResultChange
                    };
                }
                return Task.FromResult(updated);
            }, ct);
            if (updated) {
                // If updated notify about dataset writer state change
                await _writerEvents.NotifyAllAsync(
                    l => l.OnDataSetWriterStateChangeAsync(context, dataSetWriterId, writer));
            }
        }

        /// <inheritdoc/>
        public async Task UpdateWriterGroupStateAsync(string writerGroupId,
            WriterGroupState? state,
            PublisherOperationContextModel context, CancellationToken ct) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            var updated = false;
            var lastResultChange = context?.Time ?? DateTime.UtcNow;
            var group = await _groups.UpdateAsync(writerGroupId, existing => {
                var existingState = existing.State?.State;
                if (existingState != state) {
                    updated = true;
                    if (state == null) {
                        existing.State = null;
                    }
                    else {
                        existing.State = new WriterGroupStateModel {
                            State = state,
                            LastStateChange = lastResultChange
                        };
                    }
                }
                return Task.FromResult(updated);
            }, ct);
            if (updated) {
                // If updated notify about group change
                await _groupEvents.NotifyAllAsync(
                    l => l.OnWriterGroupStateChangeAsync(context, group));
            }
        }

        private readonly IDataSetEntityRepository _dataSets;
        private readonly IDataSetWriterRepository _writers;
        private readonly IWriterGroupRepository _groups;
        private readonly IPublisherEventBroker<IPublishedDataSetListener> _itemEvents;
        private readonly IPublisherEventBroker<IDataSetWriterRegistryListener> _writerEvents;
        private readonly IPublisherEventBroker<IWriterGroupRegistryListener> _groupEvents;
    }
}
