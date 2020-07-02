// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.CosmosDb.Services {
    using Microsoft.Azure.IIoT.Utils;
    using Microsoft.Azure.IIoT.Serializers;
    using Microsoft.Azure.Cosmos.Client;
    using Microsoft.Azure.Cosmos;
    using Serilog;
    using System;
    using System.Threading.Tasks;
    using System.IO;
    using System.IO.Pipelines;

    /// <summary>
    /// Provides document db and graph functionality for storage interfaces.
    /// </summary>
    public sealed class CosmosDbServiceClient : IDatabaseServer {

        /// <summary>
        /// Creates server
        /// </summary>
        /// <param name="config"></param>
        /// <param name="serializer"></param>
        /// <param name="logger"></param>
        public CosmosDbServiceClient(ICosmosDbConfig config, IJsonSerializer serializer,
            ILogger logger) {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            if (string.IsNullOrEmpty(_config?.DbConnectionString)) {
                throw new ArgumentNullException(nameof(_config.DbConnectionString));
            }
        }

        /// <inheritdoc/>
        public async Task<IDatabase> OpenAsync(string databaseId, DatabaseOptions options) {
            if (string.IsNullOrEmpty(databaseId)) {
                databaseId = "default";
            }
            var cs = ConnectionString.Parse(_config.DbConnectionString);
            var client = new CosmosClient(cs.Endpoint, cs.SharedAccessKey,
                new CosmosClientOptions {
                    Serializer = new CosmosJsonNetSerializer(_serializer),
                    ConsistencyLevel = options?.Consistency.ToConsistencyLevel()
                });
            var response = await client.CreateDatabaseIfNotExistsAsync(databaseId, 
                _config.ThroughputUnits);
            return new DocumentDatabase(response.Database, _logger);
        }

        /// <summary>
        /// Json serializer
        /// </summary>
        public class CosmosJsonNetSerializer : CosmosSerializer {

            /// <summary>
            /// Create serializer
            /// </summary>
            /// <param name="serializer"></param>
            public CosmosJsonNetSerializer(IJsonSerializer serializer) {
                _serializer = serializer;
            }

            /// <inheritdoc/>
            public override T FromStream<T>(Stream stream) {
                using (stream) {
                    if (typeof(Stream).IsAssignableFrom(typeof(T))) {
                        return (T)(object)stream;
                    }
                    using (var sr = new StreamReader(stream)) {
                        return _serializer.Deserialize<T>(sr);
                    }
                }
            }

            /// <inheritdoc/>
            public override Stream ToStream<T>(T input) {
                var pipe = new Pipe();
                _serializer.Serialize(pipe.Writer, input);
                return pipe.Reader.AsStream();
            }

            private readonly IJsonSerializer _serializer;
        }

        private readonly ICosmosDbConfig _config;
        private readonly ILogger _logger;
        private readonly IJsonSerializer _serializer;
    }
}
