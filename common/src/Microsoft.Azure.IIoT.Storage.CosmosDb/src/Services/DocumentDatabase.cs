// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.CosmosDb.Services {
    using Microsoft.Azure.IIoT.Serializers;
    using Microsoft.Azure.Cosmos;
    using Serilog;
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Threading;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Net;

    /// <summary>
    /// Provides document db database interface.
    /// </summary>
    internal sealed class DocumentDatabase : IDatabase {

        /// <summary>
        /// Creates database
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="database"></param>
        internal DocumentDatabase(Database database, ILogger logger) {
            _database = database ?? throw new ArgumentNullException(nameof(database));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _collections = new ConcurrentDictionary<string, DocumentCollection>();
        }

        /// <inheritdoc/>
        public async Task<IItemContainer> OpenContainerAsync(string id,
            ContainerOptions options) {
            return await OpenOrCreateCollectionAsync(id, options);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> ListContainersAsync(CancellationToken ct) {
            var result = new List<string>();
            var resultSetIterator = _database.GetContainerQueryIterator<ContainerProperties>();
            while (resultSetIterator.HasMoreResults) {
                foreach (var container in await resultSetIterator.ReadNextAsync()) {
                    result.Add(container.Id);
                }
            }
            return result;
        }

        /// <inheritdoc/>
        public async Task DeleteContainerAsync(string id) {
            if (string.IsNullOrEmpty(id)) {
                throw new ArgumentNullException(nameof(id));
            }
            try {
                var container = _database.GetContainer(id);
                await container.DeleteContainerAsync();
            }
            catch { }
            finally {
                _collections.TryRemove(id, out var collection);
            }
        }

        /// <inheritdoc/>
        public void Dispose() {
            _collections.Clear();
        }

        /// <summary>
        /// Create or Open collection
        /// </summary>
        /// <param name="id"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        private async Task<DocumentCollection> OpenOrCreateCollectionAsync(
            string id, ContainerOptions options) {
            if (string.IsNullOrEmpty(id)) {
                id = "default";
            }
            if (!_collections.TryGetValue(id, out var collection)) {
                var container = await EnsureCollectionExistsAsync(id, options);
                collection = _collections.GetOrAdd(id, k => new DocumentCollection(
                    container, !string.IsNullOrEmpty(options?.PartitionKey), _logger));
            }
            return collection;
        }

        /// <summary>
        /// Ensures collection exists
        /// </summary>
        /// <param name="id"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        private async Task<Container> EnsureCollectionExistsAsync(string id,
            ContainerOptions options) {

            var containerProperties = new ContainerProperties {
                Id = id,
                DefaultTimeToLive = (int?)options?.ItemTimeToLive?.TotalMilliseconds ?? -1,
                IndexingPolicy = new IndexingPolicy {
                    Automatic = true, // new RangeIndex(DataType.String) {
                                      //  Precision = -1
                                      //     })
                }
            };
            if (!string.IsNullOrEmpty(options?.PartitionKey)) {
                containerProperties.PartitionKeyPath = "/" + options.PartitionKey;
            }
            var container = await _database.CreateContainerIfNotExistsAsync(
                containerProperties);

            await CreateSprocIfNotExistsAsync(container.Container, BulkUpdateSprocName);
            await CreateSprocIfNotExistsAsync(container.Container, BulkDeleteSprocName);
            return container.Container;
        }

        internal const string BulkUpdateSprocName = "bulkUpdate";
        internal const string BulkDeleteSprocName = "bulkDelete";

        /// <summary>
        /// Create stored procedures
        /// </summary>
        /// <param name="container"></param>
        /// <param name="sprocName"></param>
        /// <returns></returns>
        private async Task CreateSprocIfNotExistsAsync(Container container, string sprocName) {
            var assembly = GetType().Assembly;
#if FALSE
            try {
                await container.Scripts.DeleteStoredProcedureAsync(sprocName);
            }
            catch (CosmosException) {}
#endif
            var resource = $"{assembly.GetName().Name}.Script.{sprocName}.js";
            using (var stream = assembly.GetManifestResourceStream(resource)) {
                if (stream == null) {
                    throw new FileNotFoundException(resource + " not found");
                }
                try {
                    await container.Scripts.ReadStoredProcedureAsync(sprocName);
                    return;
                }
                catch (CosmosException de) {
                    if (de.StatusCode != HttpStatusCode.NotFound) {
                        throw;
                    }
                }
                var sproc = new Cosmos.Scripts.StoredProcedureProperties {
                    Id = sprocName,
                    Body = stream.ReadAsString(Encoding.UTF8)
                };
                await container.Scripts.CreateStoredProcedureAsync(sproc);
            }
        }

        private readonly Database _database;
        private readonly ILogger _logger;
     //   private readonly int? _databaseThroughput;
        private readonly ConcurrentDictionary<string, DocumentCollection> _collections;
        //   private readonly IJsonSerializerSettingsProvider _jsonConfig;
    }
}
