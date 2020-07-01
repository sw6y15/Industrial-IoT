// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Repository for dataset entities
    /// </summary>
    public interface IDataSetEntityRepository {

        /// <summary>
        /// Add new event dataset to repository.
        /// The created event dataset is returned.
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="eventDataSet"></param>
        /// <param name="ct"></param>
        /// <returns>The newly created published item</returns>
        Task<PublishedDataSetEventsModel> AddEventDataSetAsync(string dataSetWriterId,
            PublishedDataSetEventsModel eventDataSet, CancellationToken ct = default);

        /// <summary>
        /// Add new variable to repository.
        /// The created variable is returned.
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="variable"></param>
        /// <param name="ct"></param>
        /// <returns>The newly created variable</returns>
        Task<PublishedDataSetVariableModel> AddDataSetVariableAsync(string dataSetWriterId,
            PublishedDataSetVariableModel variable, CancellationToken ct = default);

        /// <summary>
        /// Add a new one or update existing event dataset
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="predicate">receives existing dataset or
        /// null if not exists, return null to cancel update.
        /// </param>
        /// <param name="ct"></param>
        /// <returns>The existing or udpated writer group</returns>
        Task<PublishedDataSetEventsModel> AddOrUpdateEventDataSetAsync(string dataSetWriterId,
             Func<PublishedDataSetEventsModel, Task<PublishedDataSetEventsModel>> predicate,
             CancellationToken ct = default);

        /// <summary>
        /// Add a new one or update existing variable
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="variableId"></param>
        /// <param name="predicate">receives existing variable or
        /// null if not exists, return null to cancel update.
        /// </param>
        /// <param name="ct"></param>
        /// <returns>The existing or udpated writer group</returns>
        Task<PublishedDataSetVariableModel> AddOrUpdateDataSetVariableAsync(
            string dataSetWriterId, string variableId,
             Func<PublishedDataSetVariableModel, Task<PublishedDataSetVariableModel>> predicate,
             CancellationToken ct = default);

        /// <summary>
        /// Update event dataset
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="predicate"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<PublishedDataSetEventsModel> UpdateEventDataSetAsync(string dataSetWriterId,
            Func<PublishedDataSetEventsModel, Task<bool>> predicate,
            CancellationToken ct = default);

        /// <summary>
        /// Update dataset variable
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="variableId"></param>
        /// <param name="predicate"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<PublishedDataSetVariableModel> UpdateDataSetVariableAsync(string dataSetWriterId,
            string variableId, Func<PublishedDataSetVariableModel, Task<bool>> predicate,
            CancellationToken ct = default);

        /// <summary>
        /// Get published variable by identifier
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="variableId"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<PublishedDataSetVariableModel> FindDataSetVariableAsync(string dataSetWriterId,
            string variableId, CancellationToken ct = default);

        /// <summary>
        /// Get event dataset by identifier
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<PublishedDataSetEventsModel> FindEventDataSetAsync(string dataSetWriterId,
            CancellationToken ct = default);

        /// <summary>
        /// Delete published variable
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="variableId"></param>
        /// <param name="predicate"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<PublishedDataSetVariableModel> DeleteDataSetVariableAsync(string dataSetWriterId,
            string variableId, Func<PublishedDataSetVariableModel, Task<bool>> predicate,
            CancellationToken ct = default);

        /// <summary>
        /// Delete event data set
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="predicate"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<PublishedDataSetEventsModel> DeleteEventDataSetAsync(string dataSetWriterId,
            Func<PublishedDataSetEventsModel, Task<bool>> predicate,
            CancellationToken ct = default);

        /// <summary>
        /// Delete dataset variable
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="variableId"></param>
        /// <param name="generationId"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task DeleteDataSetVariableAsync(string dataSetWriterId,
            string variableId, string generationId, CancellationToken ct = default);

        /// <summary>
        /// Delete event dataset
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="generationId"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task DeleteEventDataSetAsync(string dataSetWriterId,
            string generationId, CancellationToken ct = default);

        /// <summary>
        /// List variables
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="query"></param>
        /// <param name="continuation"></param>
        /// <param name="maxResults"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<PublishedDataSetVariableListModel> QueryDataSetVariablesAsync(
            string dataSetWriterId, PublishedDataSetVariableQueryModel query = null,
            string continuation = null, int? maxResults = null,
            CancellationToken ct = default);

        /// <summary>
        /// Delete any entity connected to the writer
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task DeleteDataSetAsync(string dataSetWriterId, CancellationToken ct = default);
    }
}