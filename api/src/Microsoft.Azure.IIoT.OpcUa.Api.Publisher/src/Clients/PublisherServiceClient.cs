// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Clients {
    using Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Models;
    using Microsoft.Azure.IIoT.Http;
    using Microsoft.Azure.IIoT.Serializers.NewtonSoft;
    using Microsoft.Azure.IIoT.Serializers;
    using System;
    using System.Threading.Tasks;
    using System.Threading;

    /// <summary>
    /// Implementation of publisher configuration service api.
    /// </summary>
    public sealed class PublisherServiceClient : IPublisherServiceApi {

        /// <summary>
        /// Create service client
        /// </summary>
        /// <param name="httpClient"></param>
        /// <param name="config"></param>
        /// <param name="serializer"></param>
        public PublisherServiceClient(IHttpClient httpClient, IPublisherConfig config,
            ISerializer serializer) :
            this(httpClient, config?.OpcUaPublisherServiceUrl, serializer) {
        }

        /// <summary>
        /// Create service client
        /// </summary>
        /// <param name="httpClient"></param>
        /// <param name="serviceUri"></param>
        /// <param name="serializer"></param>
        public PublisherServiceClient(IHttpClient httpClient, string serviceUri,
            ISerializer serializer) {
            if (string.IsNullOrWhiteSpace(serviceUri)) {
                throw new ArgumentNullException(nameof(serviceUri),
                    "Please configure the Url of the endpoint micro service.");
            }
            _serializer = serializer ?? new NewtonSoftJsonSerializer();
            _serviceUri = serviceUri.TrimEnd('/');
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        }

        /// <inheritdoc/>
        public async Task<string> GetServiceStatusAsync(CancellationToken ct) {
            var request = _httpClient.NewRequest($"{_serviceUri}/healthz",
                Resource.Platform);
            try {
                var response = await _httpClient.GetAsync(request, ct).ConfigureAwait(false);
                response.Validate();
                return response.GetContentAsString();
            }
            catch (Exception ex) {
                return ex.Message;
            }
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterAddResponseApiModel> AddDataSetWriterAsync(
            DataSetWriterAddRequestApiModel content, CancellationToken ct) {
            if (content == null) {
                throw new ArgumentNullException(nameof(content));
            }
            var request = _httpClient.NewRequest($"{_serviceUri}/v2/writers", Resource.Platform);
            _serializer.SerializeToRequest(request, content);
            var response = await _httpClient.PutAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<DataSetWriterAddResponseApiModel>(response);
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterApiModel> GetDataSetWriterAsync(string dataSetWriterId,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            var request = _httpClient.NewRequest($"{_serviceUri}/v2/writers/{dataSetWriterId}",
                Resource.Platform);
            _serializer.SetAcceptHeaders(request);
            var response = await _httpClient.GetAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<DataSetWriterApiModel>(response);
        }

        /// <inheritdoc/>
        public async Task UpdateDataSetWriterAsync(string dataSetWriterId,
            DataSetWriterUpdateRequestApiModel content, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (content == null) {
                throw new ArgumentNullException(nameof(content));
            }
            var request = _httpClient.NewRequest($"{_serviceUri}/v2/writers/{dataSetWriterId}",
                Resource.Platform);
            _serializer.SerializeToRequest(request, content);
            var response = await _httpClient.PatchAsync(request, ct).ConfigureAwait(false);
            response.Validate();
        }

        /// <inheritdoc/>
        public async Task<DataSetAddEventResponseApiModel> AddEventDataSetAsync(
            string dataSetWriterId, DataSetAddEventRequestApiModel content, CancellationToken ct) {
            if (content == null) {
                throw new ArgumentNullException(nameof(content));
            }
            var request = _httpClient.NewRequest($"{_serviceUri}/v2/writers/{dataSetWriterId}/event",
                Resource.Platform);
            _serializer.SerializeToRequest(request, content);
            var response = await _httpClient.PutAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<DataSetAddEventResponseApiModel>(response);
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetEventsApiModel> GetEventDataSetAsync(
            string dataSetWriterId, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            var request = _httpClient.NewRequest($"{_serviceUri}/v2/writers/{dataSetWriterId}/event",
                Resource.Platform);
            _serializer.SetAcceptHeaders(request);
            var response = await _httpClient.GetAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<PublishedDataSetEventsApiModel>(response);
        }

        /// <inheritdoc/>
        public async Task UpdateEventDataSetAsync(string dataSetWriterId,
            DataSetUpdateEventRequestApiModel content, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (content == null) {
                throw new ArgumentNullException(nameof(content));
            }
            var request = _httpClient.NewRequest($"{_serviceUri}/v2/writers/{dataSetWriterId}/event",
                Resource.Platform);
            _serializer.SerializeToRequest(request, content);
            var response = await _httpClient.PatchAsync(request, ct).ConfigureAwait(false);
            response.Validate();
        }

        /// <inheritdoc/>
        public async Task RemoveEventDataSetAsync(string dataSetWriterId, string generationId,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (string.IsNullOrEmpty(generationId)) {
                throw new ArgumentNullException(nameof(generationId));
            }
            var request = _httpClient.NewRequest(
                $"{_serviceUri}/v2/writers/{dataSetWriterId}/event/{generationId}",
                Resource.Platform);
            var response = await _httpClient.DeleteAsync(request, ct).ConfigureAwait(false);
            response.Validate();
        }

        /// <inheritdoc/>
        public async Task<DataSetAddVariableResponseApiModel> AddDataSetVariableAsync(
            string dataSetWriterId, DataSetAddVariableRequestApiModel content,
            CancellationToken ct) {
            if (content == null) {
                throw new ArgumentNullException(nameof(content));
            }
            var request = _httpClient.NewRequest(
                $"{_serviceUri}/v2/writers/{dataSetWriterId}/variables", Resource.Platform);
            _serializer.SerializeToRequest(request, content);
            var response = await _httpClient.PutAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<DataSetAddVariableResponseApiModel>(response);
        }

        /// <inheritdoc/>
        public async Task UpdateDataSetVariableAsync(string dataSetWriterId, string variableId,
            DataSetUpdateVariableRequestApiModel content, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (string.IsNullOrEmpty(variableId)) {
                throw new ArgumentNullException(nameof(variableId));
            }
            if (content == null) {
                throw new ArgumentNullException(nameof(content));
            }
            var request = _httpClient.NewRequest(
                $"{_serviceUri}/v2/writers/{dataSetWriterId}/variables/{variableId}",
                Resource.Platform);
            _serializer.SerializeToRequest(request, content);
            var response = await _httpClient.PatchAsync(request, ct).ConfigureAwait(false);
            response.Validate();
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetVariableListApiModel> ListDataSetVariablesAsync(
            string dataSetWriterId, string continuation, int? pageSize, CancellationToken ct) {
            var uri = new UriBuilder($"{_serviceUri}/v2/writers/{dataSetWriterId}/variables");
            var request = _httpClient.NewRequest(uri.Uri, Resource.Platform);
            if (continuation != null) {
                request.AddHeader(HttpHeader.ContinuationToken, continuation);
            }
            if (pageSize != null) {
                request.AddHeader(HttpHeader.MaxItemCount, pageSize.ToString());
            }
            _serializer.SetAcceptHeaders(request);
            var response = await _httpClient.GetAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<PublishedDataSetVariableListApiModel>(response);
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetVariableListApiModel> QueryDataSetVariablesAsync(
            string dataSetWriterId, PublishedDataSetVariableQueryApiModel query, int? pageSize,
            CancellationToken ct) {
            var uri = new UriBuilder($"{_serviceUri}/v2/writers/{dataSetWriterId}/variables/query");
            var request = _httpClient.NewRequest(uri.Uri, Resource.Platform);
            if (pageSize != null) {
                request.AddHeader(HttpHeader.MaxItemCount, pageSize.ToString());
            }
            _serializer.SerializeToRequest(request, query);
            var response = await _httpClient.PostAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<PublishedDataSetVariableListApiModel>(response);
        }

        /// <inheritdoc/>
        public async Task RemoveDataSetVariableAsync(string dataSetWriterId, string variableId,
            string generationId, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (string.IsNullOrEmpty(variableId)) {
                throw new ArgumentNullException(nameof(variableId));
            }
            if (string.IsNullOrEmpty(generationId)) {
                throw new ArgumentNullException(nameof(generationId));
            }
            var request = _httpClient.NewRequest(
                $"{_serviceUri}/v2/writers/{dataSetWriterId}/variables/{variableId}/{generationId}",
                Resource.Platform);
            var response = await _httpClient.DeleteAsync(request, ct).ConfigureAwait(false);
            response.Validate();
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterInfoListApiModel> ListDataSetWritersAsync(
            string continuation, int? pageSize, CancellationToken ct) {
            var uri = new UriBuilder($"{_serviceUri}/v2/writers");
            var request = _httpClient.NewRequest(uri.Uri, Resource.Platform);
            if (continuation != null) {
                request.AddHeader(HttpHeader.ContinuationToken, continuation);
            }
            if (pageSize != null) {
                request.AddHeader(HttpHeader.MaxItemCount, pageSize.ToString());
            }
            _serializer.SetAcceptHeaders(request);
            var response = await _httpClient.GetAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<DataSetWriterInfoListApiModel>(response);
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterInfoListApiModel> QueryDataSetWritersAsync(
            DataSetWriterInfoQueryApiModel query, int? pageSize, CancellationToken ct) {
            var uri = new UriBuilder($"{_serviceUri}/v2/writers/query");
            var request = _httpClient.NewRequest(uri.Uri, Resource.Platform);
            if (pageSize != null) {
                request.AddHeader(HttpHeader.MaxItemCount, pageSize.ToString());
            }
            _serializer.SerializeToRequest(request, query);
            var response = await _httpClient.PostAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<DataSetWriterInfoListApiModel>(response);
        }

        /// <inheritdoc/>
        public async Task RemoveDataSetWriterAsync(string dataSetWriterId, string generationId,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (string.IsNullOrEmpty(generationId)) {
                throw new ArgumentNullException(nameof(generationId));
            }
            var request = _httpClient.NewRequest(
                $"{_serviceUri}/v2/writers/{dataSetWriterId}/{generationId}", Resource.Platform);
            var response = await _httpClient.DeleteAsync(request, ct).ConfigureAwait(false);
            response.Validate();
        }

        /// <inheritdoc/>
        public async Task<WriterGroupAddResponseApiModel> AddWriterGroupAsync(
            WriterGroupAddRequestApiModel content, CancellationToken ct) {
            if (content == null) {
                throw new ArgumentNullException(nameof(content));
            }
            var request = _httpClient.NewRequest($"{_serviceUri}/v2/groups", Resource.Platform);
            _serializer.SerializeToRequest(request, content);
            var response = await _httpClient.PutAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<WriterGroupAddResponseApiModel>(response);
        }

        /// <inheritdoc/>
        public async Task<WriterGroupApiModel> GetWriterGroupAsync(string writerGroupId,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            var request = _httpClient.NewRequest($"{_serviceUri}/v2/groups/{writerGroupId}",
                Resource.Platform);
            _serializer.SetAcceptHeaders(request);
            var response = await _httpClient.GetAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<WriterGroupApiModel>(response);
        }

        /// <inheritdoc/>
        public async Task UpdateWriterGroupAsync(string writerGroupId,
            WriterGroupUpdateRequestApiModel content, CancellationToken ct) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            if (content == null) {
                throw new ArgumentNullException(nameof(content));
            }
            var request = _httpClient.NewRequest($"{_serviceUri}/v2/groups/{writerGroupId}",
                Resource.Platform);
            _serializer.SerializeToRequest(request, content);
            var response = await _httpClient.PatchAsync(request, ct).ConfigureAwait(false);
            response.Validate();
        }

        /// <inheritdoc/>
        public async Task ActivateWriterGroupAsync(string writerGroupId, CancellationToken ct ) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            var request = _httpClient.NewRequest($"{_serviceUri}/v2/groups/{writerGroupId}/activate",
                Resource.Platform);
            _serializer.SetAcceptHeaders(request);
            var response = await _httpClient.PostAsync(request, ct).ConfigureAwait(false);
            response.Validate();
        }

        /// <inheritdoc/>
        public async Task DeactivateWriterGroupAsync(string writerGroupId, CancellationToken ct) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            var request = _httpClient.NewRequest($"{_serviceUri}/v2/groups/{writerGroupId}/deactivate",
                Resource.Platform);
            _serializer.SetAcceptHeaders(request);
            var response = await _httpClient.PostAsync(request, ct).ConfigureAwait(false);
            response.Validate();
        }

        /// <inheritdoc/>
        public async Task<WriterGroupInfoListApiModel> ListWriterGroupsAsync(
            string continuation, int? pageSize, CancellationToken ct) {
            var uri = new UriBuilder($"{_serviceUri}/v2/groups");
            var request = _httpClient.NewRequest(uri.Uri, Resource.Platform);
            if (continuation != null) {
                request.AddHeader(HttpHeader.ContinuationToken, continuation);
            }
            if (pageSize != null) {
                request.AddHeader(HttpHeader.MaxItemCount, pageSize.ToString());
            }
            _serializer.SetAcceptHeaders(request);
            var response = await _httpClient.GetAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<WriterGroupInfoListApiModel>(response);
        }

        /// <inheritdoc/>
        public async Task<WriterGroupInfoListApiModel> QueryWriterGroupsAsync(
            WriterGroupInfoQueryApiModel query, int? pageSize, CancellationToken ct) {
            var uri = new UriBuilder($"{_serviceUri}/v2/groups/query");
            var request = _httpClient.NewRequest(uri.Uri, Resource.Platform);
            if (pageSize != null) {
                request.AddHeader(HttpHeader.MaxItemCount, pageSize.ToString());
            }
            _serializer.SerializeToRequest(request, query);
            var response = await _httpClient.PostAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<WriterGroupInfoListApiModel>(response);
        }

        /// <inheritdoc/>
        public async Task RemoveWriterGroupAsync(string writerGroupId, string generationId,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            if (string.IsNullOrEmpty(generationId)) {
                throw new ArgumentNullException(nameof(generationId));
            }
            var request = _httpClient.NewRequest(
                $"{_serviceUri}/v2/groups/{writerGroupId}/{generationId}", Resource.Platform);
            var response = await _httpClient.DeleteAsync(request, ct).ConfigureAwait(false);
            response.Validate();
        }

        /// <inheritdoc/>
        public async Task<DataSetAddVariableBatchResponseApiModel> AddVariablesToDataSetWriterAsync(
            string dataSetWriterId, DataSetAddVariableBatchRequestApiModel content, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (content == null) {
                throw new ArgumentNullException(nameof(content));
            }
            var request = _httpClient.NewRequest($"{_serviceUri}/v2/bulk/writers/{dataSetWriterId}",
                Resource.Platform);
            _serializer.SerializeToRequest(request, content);
            var response = await _httpClient.PostAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<DataSetAddVariableBatchResponseApiModel>(response);
        }

        /// <inheritdoc/>
        public async Task<DataSetAddVariableBatchResponseApiModel> AddVariablesToDefaultDataSetWriterAsync(
            string endpointId, DataSetAddVariableBatchRequestApiModel content, CancellationToken ct) {
            if (string.IsNullOrEmpty(endpointId)) {
                throw new ArgumentNullException(nameof(endpointId));
            }
            if (content == null) {
                throw new ArgumentNullException(nameof(content));
            }
            var request = _httpClient.NewRequest($"{_serviceUri}/v2/bulk/endpoints/{endpointId}",
                Resource.Platform);
            _serializer.SerializeToRequest(request, content);
            var response = await _httpClient.PostAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<DataSetAddVariableBatchResponseApiModel>(response);
        }

        /// <inheritdoc/>
        public async Task<DataSetRemoveVariableBatchResponseApiModel> RemoveVariablesFromDataSetWriterAsync(
            string dataSetWriterId, DataSetRemoveVariableBatchRequestApiModel content, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (content == null) {
                throw new ArgumentNullException(nameof(content));
            }
            var request = _httpClient.NewRequest($"{_serviceUri}/v2/bulk/writers/{dataSetWriterId}/remove",
                Resource.Platform);
            _serializer.SerializeToRequest(request, content);
            var response = await _httpClient.PostAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            return _serializer.DeserializeResponse<DataSetRemoveVariableBatchResponseApiModel>(response);
        }

        private readonly IHttpClient _httpClient;
        private readonly ISerializer _serializer;
        private readonly string _serviceUri;
    }
}
