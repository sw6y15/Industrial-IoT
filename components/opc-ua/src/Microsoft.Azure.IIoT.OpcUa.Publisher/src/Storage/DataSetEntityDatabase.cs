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
    /// Dataset repository
    /// </summary>
    public class DataSetEntityDatabase : IDataSetEntityRepository {

        /// <summary>
        /// Create
        /// </summary>
        /// <param name="databaseServer"></param>
        /// <param name="config"></param>
        public DataSetEntityDatabase(IDatabaseServer databaseServer, IItemContainerConfig config) {
            var dbs = databaseServer.OpenAsync(config.DatabaseName).Result;
            var cont = dbs.OpenContainerAsync(config.ContainerName ?? "publisher").Result;
            _documents = cont.AsDocuments();
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetEventsModel> AddEventDataSetAsync(string dataSetWriterId,
            PublishedDataSetEventsModel item, CancellationToken ct) {
            if (item == null) {
                throw new ArgumentNullException(nameof(item));
            }
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            item.Id = DataSetEntityDocumentEx.GetDocumentId(dataSetWriterId);
            while (true) {
                var document = await _documents.FindAsync<DataSetEntityDocument>(item.Id, ct);
                if (document != null) {
                    throw new ConflictingResourceException($"Events {item.Id} already exists.");
                }
                try {
                    var result = await _documents.AddAsync(item.ToDocumentModel(dataSetWriterId), ct);
                    return result.Value.ToEventDataSetModel();
                }
                catch (ConflictingResourceException) {
                    // Try again
                    continue;
                }
                catch {
                    throw;
                }
            }
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetEventsModel> AddOrUpdateEventDataSetAsync(string dataSetWriterId,
            Func<PublishedDataSetEventsModel, Task<PublishedDataSetEventsModel>> predicate,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            var eventsId = DataSetEntityDocumentEx.GetDocumentId(dataSetWriterId);
            while (true) {
                var document = await _documents.FindAsync<DataSetEntityDocument>(eventsId, ct);
                var updateOrAdd = document?.Value.ToEventDataSetModel();
                var item = await predicate(updateOrAdd);
                if (item == null) {
                    return updateOrAdd;
                }
                var updated = item.ToDocumentModel(dataSetWriterId);
                if (document == null) {
                    try {
                        // Add document
                        var result = await _documents.AddAsync(updated, ct);
                        return result.Value.ToEventDataSetModel();
                    }
                    catch (ConflictingResourceException) {
                        // Conflict - try update now
                        continue;
                    }
                }
                // Try replacing
                try {
                    var result = await _documents.ReplaceAsync(document, updated, ct);
                    return result.Value.ToEventDataSetModel();
                }
                catch (ResourceOutOfDateException) {
                    continue;
                }
            }
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetEventsModel> UpdateEventDataSetAsync(string dataSetWriterId,
            Func<PublishedDataSetEventsModel, Task<bool>> predicate, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            var eventsId = DataSetEntityDocumentEx.GetDocumentId(dataSetWriterId);
            while (true) {
                var document = await _documents.FindAsync<DataSetEntityDocument>(eventsId, ct);
                if (document == null) {
                    throw new ResourceNotFoundException("Events not found");
                }
                var item = document.Value.ToEventDataSetModel();
                if (!await predicate(item)) {
                    return item;
                }
                var updated = item.ToDocumentModel(dataSetWriterId);
                try {
                    var result = await _documents.ReplaceAsync(document, updated, ct);
                    return result.Value.ToEventDataSetModel();
                }
                catch (ResourceOutOfDateException) {
                    continue;
                }
            }
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetEventsModel> FindEventDataSetAsync(string dataSetWriterId,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            var document = await _documents.FindAsync<DataSetEntityDocument>(
                DataSetEntityDocumentEx.GetDocumentId(dataSetWriterId), ct);
            if (document == null) {
                return null;
            }
            return document.Value.ToEventDataSetModel();
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetEventsModel> DeleteEventDataSetAsync(string dataSetWriterId,
            Func<PublishedDataSetEventsModel, Task<bool>> predicate, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            while (true) {
                var document = await _documents.FindAsync<DataSetEntityDocument>(
                    DataSetEntityDocumentEx.GetDocumentId(dataSetWriterId));
                if (document == null) {
                    return null;
                }
                var dataset = document.Value.ToEventDataSetModel();
                if (!await predicate(dataset)) {
                    return dataset;
                }
                try {
                    await _documents.DeleteAsync(document, ct);
                }
                catch (ResourceOutOfDateException) {
                    continue;
                }
                return dataset;
            }
        }

        /// <inheritdoc/>
        public async Task DeleteEventDataSetAsync(string dataSetWriterId, string generationId,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (string.IsNullOrEmpty(generationId)) {
                throw new ArgumentNullException(nameof(generationId));
            }
            await _documents.DeleteAsync(
                DataSetEntityDocumentEx.GetDocumentId(dataSetWriterId), ct, null, generationId);
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetVariableModel> AddDataSetVariableAsync(string dataSetWriterId,
            PublishedDataSetVariableModel item, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            if (item == null) {
                throw new ArgumentNullException(nameof(item));
            }
            var presetId = item.Id;
            while (true) {
                if (!string.IsNullOrEmpty(item.Id)) {
                    var document = await _documents.FindAsync<DataSetEntityDocument>(
                        DataSetEntityDocumentEx.GetDocumentId(dataSetWriterId, item.Id), ct);
                    if (document != null) {
                        throw new ConflictingResourceException($"Variable {item.Id} already exists.");
                    }
                }
                else {
                    item.Id = Guid.NewGuid().ToString();
                }
                try {
                    var result = await _documents.AddAsync(item.ToDocumentModel(dataSetWriterId), ct);
                    return result.Value.ToDataSetVariableModel();
                }
                catch (ConflictingResourceException) {
                    // Try again - reset to preset id or null if none was asked for
                    item.Id = presetId;
                    continue;
                }
                catch {
                    throw;
                }
            }
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetVariableModel> AddOrUpdateDataSetVariableAsync(
            string dataSetWriterId, string variableId,
            Func<PublishedDataSetVariableModel, Task<PublishedDataSetVariableModel>> predicate,
            CancellationToken ct) {
            if (string.IsNullOrEmpty(variableId)) {
                throw new ArgumentNullException(nameof(variableId));
            }
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            while (true) {
                var document = await _documents.FindAsync<DataSetEntityDocument>(
                    DataSetEntityDocumentEx.GetDocumentId(dataSetWriterId, variableId), ct);
                var updateOrAdd = document?.Value.ToDataSetVariableModel();
                var variable = await predicate(updateOrAdd);
                if (variable == null) {
                    return updateOrAdd;
                }
                variable.Id = variableId;
                var updated = variable.ToDocumentModel(dataSetWriterId);
                if (document == null) {
                    try {
                        // Add document
                        var result = await _documents.AddAsync(updated, ct);
                        return result.Value.ToDataSetVariableModel();
                    }
                    catch (ConflictingResourceException) {
                        // Conflict - try update now
                        continue;
                    }
                }
                // Try replacing
                try {
                    var result = await _documents.ReplaceAsync(document, updated, ct);
                    return result.Value.ToDataSetVariableModel();
                }
                catch (ResourceOutOfDateException) {
                    continue;
                }
            }
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetVariableModel> UpdateDataSetVariableAsync(string dataSetWriterId,
            string variableId, Func<PublishedDataSetVariableModel, Task<bool>> predicate, CancellationToken ct) {

            if (string.IsNullOrEmpty(variableId)) {
                throw new ArgumentNullException(nameof(variableId));
            }
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            while (true) {
                var document = await _documents.FindAsync<DataSetEntityDocument>(
                    DataSetEntityDocumentEx.GetDocumentId(dataSetWriterId, variableId), ct);
                if (document == null) {
                    throw new ResourceNotFoundException("Variable not found");
                }
                var variable = document.Value.ToDataSetVariableModel();
                if (!await predicate(variable)) {
                    return variable;
                }
                variable.Id = variableId;
                var updated = variable.ToDocumentModel(dataSetWriterId);
                try {
                    var result = await _documents.ReplaceAsync(document, updated, ct);
                    return result.Value.ToDataSetVariableModel();
                }
                catch (ResourceOutOfDateException) {
                    continue;
                }
            }
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetVariableModel> FindDataSetVariableAsync(string dataSetWriterId,
            string variableId, CancellationToken ct) {
            if (string.IsNullOrEmpty(variableId)) {
                throw new ArgumentNullException(nameof(variableId));
            }
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            var document = await _documents.FindAsync<DataSetEntityDocument>(
                DataSetEntityDocumentEx.GetDocumentId(dataSetWriterId, variableId), ct);
            if (document == null) {
                return null;
            }
            return document.Value.ToDataSetVariableModel();
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetVariableModel> DeleteDataSetVariableAsync(string dataSetWriterId,
            string variableId, Func<PublishedDataSetVariableModel, Task<bool>> predicate, CancellationToken ct) {
            if (string.IsNullOrEmpty(variableId)) {
                throw new ArgumentNullException(nameof(variableId));
            }
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            while (true) {
                var document = await _documents.FindAsync<DataSetEntityDocument>(
                    DataSetEntityDocumentEx.GetDocumentId(dataSetWriterId, variableId));
                if (document == null) {
                    return null;
                }
                var variable = document.Value.ToDataSetVariableModel();
                if (!await predicate(variable)) {
                    return variable;
                }
                try {
                    await _documents.DeleteAsync(document, ct);
                }
                catch (ResourceOutOfDateException) {
                    continue;
                }
                return variable;
            }
        }

        /// <inheritdoc/>
        public async Task DeleteDataSetVariableAsync(string dataSetWriterId, string variableId,
            string generationId, CancellationToken ct) {
            if (string.IsNullOrEmpty(variableId)) {
                throw new ArgumentNullException(nameof(variableId));
            }
            if (string.IsNullOrEmpty(generationId)) {
                throw new ArgumentNullException(nameof(generationId));
            }
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            await _documents.DeleteAsync(
                DataSetEntityDocumentEx.GetDocumentId(dataSetWriterId, variableId),
                ct, null, generationId);
        }

        /// <inheritdoc/>
        public async Task DeleteDataSetAsync(string dataSetWriterId, CancellationToken ct) {
            if (string.IsNullOrEmpty(dataSetWriterId)) {
                throw new ArgumentNullException(nameof(dataSetWriterId));
            }
            var queryString = $"SELECT * FROM r WHERE " +
$"r.{nameof(DataSetEntityDocument.DataSetWriterId)} = @dataSetWriterId AND " +
$"r.{nameof(DataSetEntityDocument.ClassType)} = '{DataSetEntityDocument.ClassTypeName}'";
            var client = _documents.OpenSqlClient();
            var results = client.Query<DataSetEntityDocument>(queryString,
                new Dictionary<string, object> {
                    { "@dataSetWriterId", dataSetWriterId }
                });
            await results.ForEachAsync(doc => _documents.DeleteAsync(doc, ct), ct);
        }

        /// <inheritdoc/>
        public async Task<PublishedDataSetVariableListModel> QueryDataSetVariablesAsync(
            string dataSetWriterId, PublishedDataSetVariableQueryModel query,
            string continuationToken, int? maxResults, CancellationToken ct) {
            var client = _documents.OpenSqlClient();
            var results = continuationToken != null ?
                client.Continue<DataSetEntityDocument>(continuationToken, maxResults) :
                client.Query<DataSetEntityDocument>(CreateQuery(dataSetWriterId,
                    query, out var queryParameters), queryParameters, maxResults);
            if (!results.HasMore()) {
                return new PublishedDataSetVariableListModel {
                    Variables = new List<PublishedDataSetVariableModel>()
                };
            }
            var documents = await results.ReadAsync(ct);
            return new PublishedDataSetVariableListModel {
                ContinuationToken = results.ContinuationToken,
                Variables = documents.Select(r => r.Value.ToDataSetVariableModel()).ToList()
            };
        }

        /// <summary>
        /// Create query
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="query"></param>
        /// <param name="queryParameters"></param>
        /// <returns></returns>
        private static string CreateQuery(string dataSetWriterId,
            PublishedDataSetVariableQueryModel query, out Dictionary<string, object> queryParameters) {
            queryParameters = new Dictionary<string, object>();
            var queryString = $"SELECT * FROM r WHERE ";
            if (!string.IsNullOrEmpty(dataSetWriterId)) {
                queryString +=
$"r.{nameof(DataSetEntityDocument.DataSetWriterId)} = @dataSetWriterId AND ";
                queryParameters.Add("@dataSetWriterId", dataSetWriterId);
            }
            if (query?.Attribute != null) {
                queryString +=
$"r.{nameof(DataSetEntityDocument.Attribute)} = @attribute AND ";
                queryParameters.Add("@attribute", query.Attribute.Value);
            }
            if (query?.PublishedVariableDisplayName != null) {
                queryString +=
$"r.{nameof(DataSetEntityDocument.DisplayName)} = @name AND ";
                queryParameters.Add("@name", query.PublishedVariableDisplayName);
            }
            if (query?.PublishedVariableNodeId != null) {
                queryString +=
$"r.{nameof(DataSetEntityDocument.NodeId)} = @nodeId AND ";
                queryParameters.Add("@nodeId", query.PublishedVariableNodeId);
            }
            queryString +=
$"r.{nameof(DataSetEntityDocument.ClassType)} = '{DataSetEntityDocument.ClassTypeName}' AND ";
            queryString +=
$"r.{nameof(DataSetEntityDocument.Type)} = '{DataSetEntityDocument.Variable}'";
            return queryString;
        }

        private readonly IDocuments _documents;
    }
}