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
    /// Database group repository
    /// </summary>
    public class WriterGroupDatabase : IWriterGroupRepository {

        /// <summary>
        /// Create
        /// </summary>
        /// <param name="databaseServer"></param>
        /// <param name="config"></param>
        public WriterGroupDatabase(IDatabaseServer databaseServer, IItemContainerConfig config) {
            var dbs = databaseServer.OpenAsync(config.DatabaseName).Result;
            var cont = dbs.OpenContainerAsync(config.ContainerName ?? "publisher").Result;
            _documents = cont.AsDocuments();
        }

        /// <inheritdoc/>
        public async Task<WriterGroupInfoModel> AddAsync(WriterGroupInfoModel writerGroup,
            CancellationToken ct) {
            if (writerGroup == null) {
                throw new ArgumentNullException(nameof(writerGroup));
            }
            var presetId = writerGroup.WriterGroupId;
            while (true) {
                if (!string.IsNullOrEmpty(writerGroup.WriterGroupId)) {
                    var document = await _documents.FindAsync<WriterGroupDocument>(
                    writerGroup.WriterGroupId, ct);
                    if (document != null) {
                        throw new ConflictingResourceException(
                            $"Writer Group {writerGroup.WriterGroupId} already exists.");
                    }
                }
                else {
                    writerGroup.WriterGroupId = Guid.NewGuid().ToString();
                }
                try {
                    var result = await _documents.AddAsync(writerGroup.ToDocumentModel(), ct);
                    return result.Value.ToFrameworkModel();
                }
                catch (ConflictingResourceException) {
                    // Try again - reset to preset id or null if none was asked for
                    writerGroup.WriterGroupId = presetId;
                    continue;
                }
                catch {
                    throw;
                }
            }
        }

        /// <inheritdoc/>
        public async Task<WriterGroupInfoModel> AddOrUpdateAsync(string writerGroupId,
            Func<WriterGroupInfoModel, Task<WriterGroupInfoModel>> predicate, CancellationToken ct) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            while (true) {
                var document = await _documents.FindAsync<WriterGroupDocument>(writerGroupId, ct);
                var updateOrAdd = document?.Value.ToFrameworkModel();
                var group = await predicate(updateOrAdd);
                if (group == null) {
                    return updateOrAdd;
                }
                group.WriterGroupId = writerGroupId;
                var updated = group.ToDocumentModel();
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
        public async Task<WriterGroupInfoModel> UpdateAsync(string writerGroupId,
            Func<WriterGroupInfoModel, Task<bool>> predicate, CancellationToken ct) {

            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            while (true) {
                var document = await _documents.FindAsync<WriterGroupDocument>(writerGroupId, ct);
                if (document == null) {
                    throw new ResourceNotFoundException("Writer group not found");
                }
                var group = document.Value.ToFrameworkModel();
                if (!await predicate(group)) {
                    return group;
                }
                group.WriterGroupId = writerGroupId;
                var updated = group.ToDocumentModel();
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
        public async Task<WriterGroupInfoModel> FindAsync(string writerGroupId,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            var document = await _documents.FindAsync<WriterGroupDocument>(
                writerGroupId, ct);
            if (document == null) {
                return null;
            }
            return document.Value.ToFrameworkModel();
        }

        /// <inheritdoc/>
        public async Task<WriterGroupInfoListModel> QueryAsync(WriterGroupInfoQueryModel query,
            string continuationToken, int? maxResults, CancellationToken ct) {
            var client = _documents.OpenSqlClient();
            var results = continuationToken != null ?
                client.Continue<WriterGroupDocument>(continuationToken, maxResults) :
                client.Query<WriterGroupDocument>(CreateQuery(query, out var queryParameters),
                    queryParameters, maxResults);
            if (!results.HasMore()) {
                return new WriterGroupInfoListModel {
                    WriterGroups = new List<WriterGroupInfoModel>()
                };
            }
            var documents = await results.ReadAsync(ct);
            return new WriterGroupInfoListModel {
                ContinuationToken = results.ContinuationToken,
                WriterGroups = documents.Select(r => r.Value.ToFrameworkModel()).ToList()
            };
        }

        /// <inheritdoc/>
        public async Task<WriterGroupInfoModel> DeleteAsync(string writerGroupId,
            Func<WriterGroupInfoModel, Task<bool>> predicate, CancellationToken ct) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            while (true) {
                var document = await _documents.FindAsync<WriterGroupDocument>(
                    writerGroupId);
                if (document == null) {
                    return null;
                }
                var group = document.Value.ToFrameworkModel();
                if (!await predicate(group)) {
                    return group;
                }
                try {
                    await _documents.DeleteAsync(document, ct);
                }
                catch (ResourceOutOfDateException) {
                    continue;
                }
                return group;
            }
        }

        /// <inheritdoc/>
        public async Task DeleteAsync(string writerGroupId, string generationId,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(writerGroupId)) {
                throw new ArgumentNullException(nameof(writerGroupId));
            }
            if (string.IsNullOrEmpty(generationId)) {
                throw new ArgumentNullException(nameof(generationId));
            }
            await _documents.DeleteAsync(writerGroupId, ct, null, generationId);
        }

        /// <summary>
        /// Create query
        /// </summary>
        /// <param name="query"></param>
        /// <param name="queryParameters"></param>
        /// <returns></returns>
        private static string CreateQuery(WriterGroupInfoQueryModel query,
            out Dictionary<string, object> queryParameters) {
            queryParameters = new Dictionary<string, object>();
            var queryString = $"SELECT * FROM r WHERE ";
            if (query?.GroupVersion != null) {
                queryString +=
$"r.{nameof(WriterGroupDocument.GroupVersion)} = @version AND ";
                queryParameters.Add("@version", query.GroupVersion.Value);
            }
            if (query?.MessageType != null) {
                queryString +=
$"r.{nameof(WriterGroupDocument.MessageType)} = @messageType AND ";
                queryParameters.Add("@messageType", query.MessageType.Value);
            }
            if (query?.Priority != null) {
                queryString +=
$"r.{nameof(WriterGroupDocument.Priority)} = @priority AND ";
                queryParameters.Add("@priority", query.Priority.Value);
            }
            if (query?.SiteId != null) {
                queryString +=
$"r.{nameof(WriterGroupDocument.SiteId)} = @siteId AND ";
                queryParameters.Add("@siteId", query.SiteId);
            }
            if (query?.Name != null) {
                queryString +=
$"r.{nameof(WriterGroupDocument.Name)} = @name AND ";
                queryParameters.Add("@name", query.Name);
            }
            queryString +=
$"r.{nameof(WriterGroupDocument.ClassType)} = '{WriterGroupDocument.ClassTypeName}'";
            return queryString;
        }

        private readonly IDocuments _documents;
    }
}