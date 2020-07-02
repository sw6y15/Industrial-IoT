// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.CosmosDb.Services {
    using Microsoft.Azure.IIoT.Utils;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Cosmos.Linq;
    using Serilog;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Document query client
    /// </summary>
    internal sealed class DocumentQuery : ISqlClient {

        /// <summary>
        /// Create document query client
        /// </summary>
        /// <param name="container"></param>
        /// <param name="partitioned"></param>
        /// <param name="logger"></param>
        internal DocumentQuery(Container container, bool partitioned, ILogger logger) {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _container = container ?? throw new ArgumentNullException(nameof(container));
            _partitioned = partitioned;
        }

        /// <inheritdoc/>
        public IResultFeed<IDocumentInfo<T>> Query<T>(string queryString,
            IDictionary<string, object> parameters, int? pageSize, string partitionKey) {
            if (string.IsNullOrEmpty(queryString)) {
                throw new ArgumentNullException(nameof(queryString));
            }

            var query = new QueryDefinition(queryString);
            foreach (var item in parameters) {
                query = query.WithParameter(item.Key, item.Value);
            }

            var pk = _partitioned || string.IsNullOrEmpty(partitionKey) ? 
                (PartitionKey?)null : new PartitionKey(partitionKey);
            var result = _container.GetItemQueryIterator<T>(query, null, 
                new QueryRequestOptions {
                    PartitionKey = pk,
                    // ...
                    MaxItemCount = pageSize ?? -1
                });

            return new DocumentInfoFeed<T>(result, _logger);
        }

        /// <inheritdoc/>
        public IResultFeed<IDocumentInfo<T>> Continue<T>(string continuationToken,
            int? pageSize, string partitionKey) {
            if (string.IsNullOrEmpty(continuationToken)) {
                throw new ArgumentNullException(nameof(continuationToken));
            }

            var pk = _partitioned || string.IsNullOrEmpty(partitionKey) ?
                (PartitionKey?)null : new PartitionKey(partitionKey);
            var result = _container.GetItemQueryIterator<T>((string)null, continuationToken,
                new QueryRequestOptions {
                    PartitionKey = pk,
                    // ...
                    MaxItemCount = pageSize ?? -1
                });

            return new DocumentInfoFeed<T>(result, _logger);
        }

        /// <inheritdoc/>
        public async Task DropAsync<T>(string queryString,
            IDictionary<string, object> parameters, string partitionKey,
            CancellationToken ct) {

            var query = new QueryDefinition(queryString);
            foreach (var item in parameters) {
                query = query.WithParameter(item.Key, item.Value);
            }

            var pk = _partitioned || string.IsNullOrEmpty(partitionKey) ?
                (PartitionKey?)null : new PartitionKey(partitionKey);
            var result = _container.GetItemQueryIterator<T>(query, null,
                new QueryRequestOptions {
                    PartitionKey = pk,
                    // ...
                    MaxItemCount = -1
                });

            while (result.HasMoreResults) {
                var results = await result.ReadNextAsync(ct);
                foreach (var toDelete in results) {
                    await Container.DeleteItemAsync<T>(toDelete.Id, pk);
                }
            }
        }

        public void Dispose() {
        }

        private readonly Container _container;
        private readonly bool _partitioned;
        private readonly ILogger _logger;
    }
}
