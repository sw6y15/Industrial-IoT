// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Storage.Default {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.Exceptions;
    using Microsoft.Azure.IIoT.Storage;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Database writer repository
    /// </summary>
    public class DataSetWriterDatabase : IDataSetWriterRepository {

        /// <summary>
        /// Create
        /// </summary>
        /// <param name="databaseServer"></param>
        /// <param name="config"></param>
        public DataSetWriterDatabase(IDatabaseServer databaseServer, IItemContainerConfig config) {
            var dbs = databaseServer.OpenAsync(config.DatabaseName).Result;
            var cont = dbs.OpenContainerAsync(config.ContainerName ?? "publisher").Result;
            _documents = cont.AsDocuments();
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterInfoModel> AddAsync(DataSetWriterInfoModel writer,
            CancellationToken ct) {
            if (writer == null) {
                throw new ArgumentNullException(nameof(writer));
            }
            var presetId = writer.DataSetWriterId;
            while (true) {
                if (!string.IsNullOrEmpty(writer.DataSetWriterId)) {
                    var document = await _documents.FindAsync<DataSetWriterDocument>(
                        writer.DataSetWriterId, ct);
                    if (document != null) {
                        throw new ConflictingResourceException(
                            $"Dataset Writer {writer.DataSetWriterId} already exists.");
                    }
                }
                else {
                    writer.DataSetWriterId = Guid.NewGuid().ToString();
                }
                try {
                    var result = await _documents.AddAsync(writer.ToDocumentModel(), ct);
                    return result.Value.ToFrameworkModel();
                }
                catch (ConflictingResourceException) {
                    // Try again - reset to preset id or null if none was asked for
                    writer.DataSetWriterId = presetId;
                    continue;
                }
                catch {
                    throw;
                }
            }
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterInfoModel> AddOrUpdateAsync(string writerId,
            Func<DataSetWriterInfoModel, Task<DataSetWriterInfoModel>> predicate, CancellationToken ct) {
            if (string.IsNullOrEmpty(writerId)) {
                throw new ArgumentNullException(nameof(writerId));
            }
            while (true) {
                var document = await _documents.FindAsync<DataSetWriterDocument>(writerId, ct);
                var updateOrAdd = document?.Value.ToFrameworkModel();
                var writer = await predicate(updateOrAdd);
                if (writer == null) {
                    return updateOrAdd;
                }
                writer.DataSetWriterId = writerId;
                var updated = writer.ToDocumentModel();
                if (document == null) {
                    try {
                        // Add document
                        var result = await _documents.AddAsync(updated, ct);
                        return result.Value.ToFrameworkModel();
                    }
                    catch (ConflictingResourceException) {
                        // Conflict - try update now
                        continue;
                    }
                }
                // Try replacing
                try {
                    var result = await _documents.ReplaceAsync(document, updated, ct);
                    return result.Value.ToFrameworkModel();
                }
                catch (ResourceOutOfDateException) {
                    continue;
                }
            }
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterInfoModel> UpdateAsync(string writerId,
            Func<DataSetWriterInfoModel, Task<bool>> predicate, CancellationToken ct) {

            if (string.IsNullOrEmpty(writerId)) {
                throw new ArgumentNullException(nameof(writerId));
            }
            while (true) {
                var document = await _documents.FindAsync<DataSetWriterDocument>(writerId, ct);
                if (document == null) {
                    throw new ResourceNotFoundException("Dataset Writer not found");
                }
                var writer = document.Value.ToFrameworkModel();
                if (!await predicate(writer)) {
                    return writer;
                }
                writer.DataSetWriterId = writerId;
                var updated = writer.ToDocumentModel();
                try {
                    var result = await _documents.ReplaceAsync(document, updated, ct);
                    return result.Value.ToFrameworkModel();
                }
                catch (ResourceOutOfDateException) {
                    continue;
                }
            }
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterInfoModel> FindAsync(string writerId, CancellationToken ct) {
            if (string.IsNullOrEmpty(writerId)) {
                throw new ArgumentNullException(nameof(writerId));
            }
            var document = await _documents.FindAsync<DataSetWriterDocument>(writerId, ct);
            if (document == null) {
                return null;
            }
            return document.Value.ToFrameworkModel();
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterInfoListModel> QueryAsync(DataSetWriterInfoQueryModel query,
            string continuationToken, int? maxResults, CancellationToken ct) {
            var client = _documents.OpenSqlClient();
            var results = continuationToken != null ?
                client.Continue<DataSetWriterDocument>(continuationToken, maxResults) :
                client.Query<DataSetWriterDocument>(CreateQuery(query, out var queryParameters),
                    queryParameters, maxResults);
            if (!results.HasMore()) {
                return new DataSetWriterInfoListModel {
                    DataSetWriters = new List<DataSetWriterInfoModel>()
                };
            }
            var documents = await results.ReadAsync(ct);
            return new DataSetWriterInfoListModel {
                ContinuationToken = results.ContinuationToken,
                DataSetWriters = documents.Select(r => r.Value.ToFrameworkModel()).ToList()
            };
        }

        /// <inheritdoc/>
        public async Task<DataSetWriterInfoModel> DeleteAsync(string writerId,
            Func<DataSetWriterInfoModel, Task<bool>> predicate, CancellationToken ct) {
            if (string.IsNullOrEmpty(writerId)) {
                throw new ArgumentNullException(nameof(writerId));
            }
            while (true) {
                var document = await _documents.FindAsync<DataSetWriterDocument>(
                    writerId);
                if (document == null) {
                    return null;
                }
                var writer = document.Value.ToFrameworkModel();
                if (!await predicate(writer)) {
                    return writer;
                }
                try {
                    await _documents.DeleteAsync(document, ct);
                }
                catch (ResourceOutOfDateException) {
                    continue;
                }
                return writer;
            }
        }

        /// <inheritdoc/>
        public async Task DeleteAsync(string writerId, string generationId,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(writerId)) {
                throw new ArgumentNullException(nameof(writerId));
            }
            if (string.IsNullOrEmpty(generationId)) {
                throw new ArgumentNullException(nameof(generationId));
            }
            await _documents.DeleteAsync(writerId, ct, null, generationId);
        }

        /// <summary>
        /// Create query
        /// </summary>
        /// <param name="query"></param>
        /// <param name="queryParameters"></param>
        /// <returns></returns>
        private static string CreateQuery(DataSetWriterInfoQueryModel query,
            out Dictionary<string, object> queryParameters) {
            queryParameters = new Dictionary<string, object>();
            var queryString = $"SELECT * FROM r WHERE ";
            if (query?.WriterGroupId != null) {
                queryString +=
$"r.{nameof(DataSetWriterDocument.WriterGroupId)} = @groupId AND ";
                queryParameters.Add("@groupId", query.WriterGroupId);
            }
            if (query?.EndpointId != null) {
                queryString +=
$"r.{nameof(DataSetWriterDocument.EndpointId)} = @endpoint AND ";
                queryParameters.Add("@endpoint", query.EndpointId);
            }
            if (query?.DataSetName != null) {
                queryString +=
$"r.{nameof(DataSetWriterDocument.DataSetName)} = @name AND ";
                queryParameters.Add("@name", query.DataSetName);
            }
            if (query?.ExcludeDisabled == true) {
                queryString +=
$"r.{nameof(DataSetWriterDocument.IsDisabled)} = false AND ";
            }
            queryString +=
$"r.{nameof(DataSetWriterDocument.ClassType)} = '{DataSetWriterDocument.ClassTypeName}'";
            return queryString;
        }

        private readonly IDocuments _documents;
    }
}