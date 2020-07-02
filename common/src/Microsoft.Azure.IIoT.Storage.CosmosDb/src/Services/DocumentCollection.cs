// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.CosmosDb.Services {
    using Microsoft.Azure.IIoT.Utils;
    using Microsoft.Azure.IIoT.Exceptions;
    using Microsoft.Azure.IIoT.Http.Exceptions;
    using Microsoft.Azure.IIoT.Http;
    using Microsoft.Azure.Cosmos;
    using Serilog;
    using System;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Wraps a cosmos db container
    /// </summary>
    internal sealed class DocumentCollection : IItemContainer, IDocuments {

        /// <inheritdoc/>
        public string Name => Container.Id;
         
        /// <summary>
        /// Wrapped document collection instance
        /// </summary>
        internal Container Container { get; }

        /// <summary>
        /// Create collection
        /// </summary>
        /// <param name="partitioned"></param>
        /// <param name="container"></param>
        /// <param name="logger"></param>
        internal DocumentCollection(Container container, bool partitioned,
            ILogger logger) {
            Container = container ?? throw new ArgumentNullException(nameof(container));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _partitioned = partitioned;
        }

        /// <inheritdoc/>
        public IDocuments AsDocuments() {
            return this;
        }

        /// <inheritdoc/>
        public ISqlClient OpenSqlClient() {
            return new DocumentQuery(Container, _partitioned, _logger);
        }

        /// <inheritdoc/>
        public async Task<IDocumentInfo<T>> FindAsync<T>(string id, CancellationToken ct,
            OperationOptions options) {
            if (string.IsNullOrEmpty(id)) {
                throw new ArgumentNullException(nameof(id));
            }
            try {
                return await Retry.WithExponentialBackoff(_logger, ct, async () => {
                    try {
                        return new DocumentInfo<T>(await Container.ReadItemAsync<T>(id, 
                            options.ToPartitionKey(_partitioned), null, ct));
                    }
                    catch (Exception ex) {
                        FilterException(ex);
                        return null;
                    }
                });
            }
            catch (ResourceNotFoundException) {
                return null;
            }
        }

        /// <inheritdoc/>
        public async Task<IDocumentInfo<T>> UpsertAsync<T>(T newItem,
            CancellationToken ct, string id, OperationOptions options, string etag) {
            return await Retry.WithExponentialBackoff(_logger, ct, async () => {
                try {
                    return new DocumentInfo<T>(await Container.UpsertItemAsync<T>(
                        newItem,
                        options.ToPartitionKey(_partitioned), new ItemRequestOptions {
                            IfMatchEtag = etag
                        }, ct));
                }
                catch (Exception ex) {
                    FilterException(ex);
                    return null;
                }
            });
        }

        /// <inheritdoc/>
        public async Task<IDocumentInfo<T>> ReplaceAsync<T>(IDocumentInfo<T> existing,
            T newItem, CancellationToken ct, OperationOptions options) {
            if (existing == null) {
                throw new ArgumentNullException(nameof(existing));
            }
            options ??= new OperationOptions();
            options.PartitionKey = existing.PartitionKey;
            return await Retry.WithExponentialBackoff(_logger, ct, async () => {
                try {
                    return new DocumentInfo<T>(await Container.ReplaceItemAsync<T>(
                        newItem, existing.Id,
                        options.ToPartitionKey(_partitioned), new ItemRequestOptions {
                            IfMatchEtag = existing.Etag
                        }, ct));
                }
                catch (Exception ex) {
                    FilterException(ex);
                    return null;
                }
            });
        }

        /// <inheritdoc/>
        public async Task<IDocumentInfo<T>> AddAsync<T>(T newItem, CancellationToken ct,
            string id, OperationOptions options) {
            return await Retry.WithExponentialBackoff(_logger, ct, async () => {
                try {
                    var result = await Container.CreateItemAsync<T>(
                        newItem, options.ToPartitionKey(_partitioned), null, ct);

                    return new DocumentInfo<T>(result);
                }
                catch (Exception ex) {
                    FilterException(ex);
                    return null;
                }
            });
        }

        /// <inheritdoc/>
        public Task DeleteAsync<T>(IDocumentInfo<T> item, CancellationToken ct,
            OperationOptions options) {
            if (item == null) {
                throw new ArgumentNullException(nameof(item));
            }
            options ??= new OperationOptions();
            options.PartitionKey = item.PartitionKey;
            return DeleteAsync<T>(item.Id, ct, options, item.Etag);
        }

        /// <inheritdoc/>
        public async Task DeleteAsync<T>(string id, CancellationToken ct,
            OperationOptions options, string etag) {
            if (string.IsNullOrEmpty(id)) {
                throw new ArgumentNullException(nameof(id));
            }
            await Retry.WithExponentialBackoff(_logger, ct, async () => {
                try {
                    await Container.DeleteItemAsync<T>(id, 
                        options.ToPartitionKey(_partitioned),
                        new ItemRequestOptions { IfMatchEtag = etag, }, ct);
                }
                catch (Exception ex) {
                    FilterException(ex);
                    return;
                }
            });
        }

        /// <summary>
        /// Filter exceptions
        /// </summary>
        /// <param name="ex"></param>
        /// <returns></returns>
        internal static void FilterException(Exception ex) {
            if (ex is HttpResponseException re) {
                re.StatusCode.Validate(re.Message);
            }
            else if (ex is CosmosException dce) {
                if (dce.StatusCode == (HttpStatusCode)429) {
                    throw new TemporarilyBusyException(dce.Message, dce, dce.RetryAfter);
                }
                dce.StatusCode.Validate(dce.Message, dce);
            }
            else {
                throw ex;
            }
        }

        private readonly ILogger _logger;
        private readonly bool _partitioned;
    }
}
