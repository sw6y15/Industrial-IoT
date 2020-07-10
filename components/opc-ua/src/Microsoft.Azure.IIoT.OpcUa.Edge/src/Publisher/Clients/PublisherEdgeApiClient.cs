// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Clients {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Clients;
    using Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Models;
    using Microsoft.Azure.IIoT.Auth;
    using Microsoft.Azure.IIoT.Serializers;
    using Microsoft.Azure.IIoT.Http;
    using System;
    using System.Net.Http.Headers;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Produces writer group and reconfigures the existing group
    /// </summary>
    public class PublisherEdgeApiClient : IDataSetWriterRegistryEdgeClient {

        /// <summary>
        /// Create connector
        /// </summary>
        /// <param name="httpClient"></param>
        /// <param name="tokenProvider"></param>
        /// <param name="serializer"></param>
        public PublisherEdgeApiClient(IHttpClient httpClient, ISasTokenGenerator tokenProvider,
            ISerializer serializer) {
            _tokenProvider = tokenProvider ?? throw new ArgumentNullException(nameof(tokenProvider));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterModel> GetDataSetWriterAsync(string serviceUrl,
            string dataSetWriterId, CancellationToken ct) {
            var uri = serviceUrl?.TrimEnd('/');
            if (uri == null) {
                throw new ArgumentNullException(nameof(serviceUrl));
            }
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            var request = _httpClient.NewRequest($"{uri}/v2/writers/{dataSetWriterId}");
            var token = await _tokenProvider.GenerateTokenAsync(request.Uri.ToString());
            request.Headers.Authorization = AuthenticationHeaderValue.Parse(token);
            _serializer.SetAcceptHeaders(request);
            var response = await _httpClient.GetAsync(request, ct).ConfigureAwait(false);
            response.Validate();
            var result = _serializer.DeserializeResponse<DataSetWriterApiModel>(
                response);
            return result.ToServiceModel();
        }

        private readonly ISasTokenGenerator _tokenProvider;
        private readonly ISerializer _serializer;
        private readonly IHttpClient _httpClient;
    }
}