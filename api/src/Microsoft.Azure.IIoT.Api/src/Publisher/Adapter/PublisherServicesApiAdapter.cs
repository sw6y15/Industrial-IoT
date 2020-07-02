// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Clients {
    using Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher;
    using System;
    using System.Threading.Tasks;
    using System.Threading;

    /// <summary>
    /// Implements node and publish services as adapter on top of api.
    /// </summary>
    public sealed class PublishServicesApiAdapter : IDataSetWriterRegistry,
        IWriterGroupRegistry, IDataSetBatchOperations {

        /// <summary>
        /// Create adapter
        /// </summary>
        /// <param name="client"></param>
        public PublishServicesApiAdapter(IPublisherServiceApi client) {
            _client = client ?? throw new ArgumentNullException(nameof(client));
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterAddResultModel> AddDataSetWriterAsync(
            DataSetWriterAddRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            var result = await _client.AddDataSetWriterAsync(
                request.ToApiModel(), ct);
            return result.ToServiceModel();
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterModel> GetDataSetWriterAsync(
            string dataSetWriterId, CancellationToken ct) {
            var result = await _client.GetDataSetWriterAsync(dataSetWriterId, ct);
            return result.ToServiceModel();
        }

        /// <inheritdoc/>
        public async Task UpdateDataSetWriterAsync(string dataSetWriterId,
            DataSetWriterUpdateRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            await _client.UpdateDataSetWriterAsync(dataSetWriterId,
                request.ToApiModel(), ct);
        }

        /// <inheritdoc/>
        public async Task<DataSetAddEventResultModel> AddEventDataSetAsync(
            string dataSetWriterId, DataSetAddEventRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            var result = await _client.AddEventDataSetAsync(dataSetWriterId,
                request.ToApiModel(), ct);
            return result.ToServiceModel();
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetEventsModel> GetEventDataSetAsync(
            string dataSetWriterId, CancellationToken ct) {
            var result = await _client.GetEventDataSetAsync(dataSetWriterId, ct);
            return result.ToServiceModel();
        }

        /// <inheritdoc/>
        public async Task UpdateEventDataSetAsync(string dataSetWriterId,
            DataSetUpdateEventRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            await _client.UpdateEventDataSetAsync(dataSetWriterId,
                request.ToApiModel(), ct);
        }

        /// <inheritdoc/>
        public async Task RemoveEventDataSetAsync(string dataSetWriterId,
            string generationId, PublisherOperationContextModel context,
            CancellationToken ct) {
            await _client.RemoveEventDataSetAsync(dataSetWriterId,
                generationId, ct);
        }

        /// <inheritdoc/>
        public async Task<DataSetAddVariableResultModel> AddDataSetVariableAsync(
            string dataSetWriterId, DataSetAddVariableRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            var result = await _client.AddDataSetVariableAsync(dataSetWriterId,
                request.ToApiModel(), ct);
            return result.ToServiceModel();
        }

        /// <inheritdoc/>
        public async Task UpdateDataSetVariableAsync(string dataSetWriterId,
            string variableId, DataSetUpdateVariableRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            await _client.UpdateDataSetVariableAsync(dataSetWriterId,
                variableId, request.ToApiModel(), ct);
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetVariableListModel> ListDataSetVariablesAsync(
            string dataSetWriterId, string continuation, int? pageSize,
            CancellationToken ct) {
            var result = await _client.ListDataSetVariablesAsync(dataSetWriterId,
                continuation, pageSize, ct);
            return result.ToServiceModel();
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetVariableListModel> QueryDataSetVariablesAsync(
            string dataSetWriterId, PublishedDataSetVariableQueryModel query, int? pageSize,
            CancellationToken ct) {
            var result = await _client.QueryDataSetVariablesAsync(dataSetWriterId,
                query.ToApiModel(), pageSize, ct);
            return result.ToServiceModel();
        }

        /// <inheritdoc/>
        public async Task RemoveDataSetVariableAsync(string dataSetWriterId,
            string variableId, string generationId,
            PublisherOperationContextModel context, CancellationToken ct) {
            await _client.RemoveDataSetVariableAsync(dataSetWriterId, variableId,
                generationId, ct);
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterInfoListModel> ListDataSetWritersAsync(
            string continuation, int? pageSize, CancellationToken ct) {
            var result = await _client.ListDataSetWritersAsync(continuation,
                pageSize, ct);
            return result.ToServiceModel();
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterInfoListModel> QueryDataSetWritersAsync(
            DataSetWriterInfoQueryModel query, int? pageSize, CancellationToken ct) {
            var result = await _client.QueryDataSetWritersAsync(
                query.ToApiModel(), pageSize, ct);
            return result.ToServiceModel();
        }

        /// <inheritdoc/>
        public async Task RemoveDataSetWriterAsync(string dataSetWriterId,
            string generationId, PublisherOperationContextModel context,
            CancellationToken ct) {
            await _client.RemoveDataSetWriterAsync(dataSetWriterId, generationId, ct);
        }

        /// <inheritdoc/>
        public async Task<WriterGroupAddResultModel> AddWriterGroupAsync(
            WriterGroupAddRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            var result = await _client.AddWriterGroupAsync(
                request.ToApiModel(), ct);
            return result.ToServiceModel();
        }

        /// <inheritdoc/>
        public async Task ActivateWriterGroupAsync(string writerGroupId,
            PublisherOperationContextModel context, CancellationToken ct) {
            await _client.ActivateWriterGroupAsync(writerGroupId, ct);
        }

        /// <inheritdoc/>
        public async Task DeactivateWriterGroupAsync(string writerGroupId,
            PublisherOperationContextModel context, CancellationToken ct) {
            await _client.DeactivateWriterGroupAsync(writerGroupId, ct);
        }

        /// <inheritdoc/>
        public async Task<WriterGroupModel> GetWriterGroupAsync(string writerGroupId,
            CancellationToken ct) {
            var result = await _client.GetWriterGroupAsync(writerGroupId, ct);
            return result.ToServiceModel();
        }

        /// <inheritdoc/>
        public async Task UpdateWriterGroupAsync(string writerGroupId,
            WriterGroupUpdateRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            await _client.UpdateWriterGroupAsync(writerGroupId,
                request.ToApiModel(), ct);
        }

        /// <inheritdoc/>
        public async Task<WriterGroupInfoListModel> ListWriterGroupsAsync(
            string continuation, int? pageSize, CancellationToken ct) {
            var result = await _client.ListWriterGroupsAsync(continuation,
                pageSize, ct);
            return result.ToServiceModel();
        }

        /// <inheritdoc/>
        public async Task<WriterGroupInfoListModel> QueryWriterGroupsAsync(
            WriterGroupInfoQueryModel query, int? pageSize, CancellationToken ct) {
            var result = await _client.QueryWriterGroupsAsync(
                query.ToApiModel(), pageSize, ct);
            return result.ToServiceModel();
        }

        /// <inheritdoc/>
        public async Task RemoveWriterGroupAsync(string writerGroupId,
            string generationId, PublisherOperationContextModel context,
            CancellationToken ct) {
            await _client.RemoveWriterGroupAsync(writerGroupId,
                generationId, ct);
        }

        /// <inheritdoc/>
        public async Task<DataSetAddVariableBatchResultModel> AddVariablesToDataSetWriterAsync(
            string dataSetWriterId, DataSetAddVariableBatchRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            var result = await _client.AddVariablesToDataSetWriterAsync(dataSetWriterId,
                request.ToApiModel(), ct);
            return result.ToServiceModel();
        }

        /// <inheritdoc/>
        public async Task<DataSetAddVariableBatchResultModel> AddVariablesToDefaultDataSetWriterAsync(
            string endpointId, DataSetAddVariableBatchRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            var result = await _client.AddVariablesToDefaultDataSetWriterAsync(endpointId,
                request.ToApiModel(), ct);
            return result.ToServiceModel();
        }

        /// <inheritdoc/>
        public async Task<DataSetRemoveVariableBatchResultModel> RemoveVariablesFromDataSetWriterAsync(
            string dataSetWriterId, DataSetRemoveVariableBatchRequestModel request,
            PublisherOperationContextModel context, CancellationToken ct) {
            var result = await _client.RemoveVariablesFromDataSetWriterAsync(dataSetWriterId,
                request.ToApiModel(), ct);
            return result.ToServiceModel();
        }

        private readonly IPublisherServiceApi _client;
    }
}
