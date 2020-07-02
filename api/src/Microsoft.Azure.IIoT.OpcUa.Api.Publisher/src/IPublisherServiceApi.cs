// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Api.Publisher {
    using Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Models;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Represents OPC twin service api functions
    /// </summary>
    public interface IPublisherServiceApi {

        /// <summary>
        /// Returns status of the service
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<string> GetServiceStatusAsync(CancellationToken ct = default);

        /// <summary>
        /// Register new dataset writer
        /// </summary>
        /// <param name="request"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<DataSetWriterAddResponseApiModel> AddDataSetWriterAsync(
            DataSetWriterAddRequestApiModel request,
            CancellationToken ct = default);

        /// <summary>
        /// Read full dataset writer model which includes all
        /// dataset members if there are any.
        /// </summary>
        /// <param name="dataSetWriterId">The dataSetWriterId</param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<DataSetWriterApiModel> GetDataSetWriterAsync(
            string dataSetWriterId, CancellationToken ct = default);

        /// <summary>
        /// Update an existing application, e.g. server
        /// certificate, or additional capabilities.
        /// </summary>
        /// <param name="dataSetWriterId">The dataSetWriterId</param>
        /// <param name="request"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task UpdateDataSetWriterAsync(string dataSetWriterId,
            DataSetWriterUpdateRequestApiModel request,
            CancellationToken ct = default);

        /// <summary>
        /// Register new event based dataset
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="request"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<DataSetAddEventResponseApiModel> AddEventDataSetAsync(
            string dataSetWriterId, DataSetAddEventRequestApiModel request,
            CancellationToken ct = default);

        /// <summary>
        /// Read full event set if any.
        /// </summary>
        /// <param name="dataSetWriterId">The event set Id</param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<PublishedDataSetEventsApiModel> GetEventDataSetAsync(
            string dataSetWriterId, CancellationToken ct = default);

        /// <summary>
        /// Update an existing event set.
        /// </summary>
        /// <param name="dataSetWriterId">The event set Id</param>
        /// <param name="request"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task UpdateEventDataSetAsync(string dataSetWriterId,
            DataSetUpdateEventRequestApiModel request,
            CancellationToken ct = default);

        /// <summary>
        /// Unregister eventset and remove from dataset.
        /// </summary>
        /// <param name="dataSetWriterId">The event set Id</param>
        /// <param name="generationId"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task RemoveEventDataSetAsync(string dataSetWriterId,
            string generationId, CancellationToken ct = default);

        /// <summary>
        /// Register new dataset variable
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="request"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<DataSetAddVariableResponseApiModel> AddDataSetVariableAsync(
            string dataSetWriterId, DataSetAddVariableRequestApiModel request,
            CancellationToken ct = default);

        /// <summary>
        /// Update an existing variable info.
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="variableId">The variableId</param>
        /// <param name="request"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task UpdateDataSetVariableAsync(string dataSetWriterId,
            string variableId, DataSetUpdateVariableRequestApiModel request,
            CancellationToken ct = default);

        /// <summary>
        /// List all dataset variables or continue find query.
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="continuation"></param>
        /// <param name="pageSize"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<PublishedDataSetVariableListApiModel> ListDataSetVariablesAsync(
            string dataSetWriterId, string continuation = null,
            int? pageSize = null, CancellationToken ct = default);

        /// <summary>
        /// Query variables in dataset.
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="query"></param>
        /// <param name="pageSize"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<PublishedDataSetVariableListApiModel> QueryDataSetVariablesAsync(
            string dataSetWriterId, PublishedDataSetVariableQueryApiModel query,
            int? pageSize = null, CancellationToken ct = default);

        /// <summary>
        /// Unregister dataset variable and remove from dataset.
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="variableId"></param>
        /// <param name="generationId"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task RemoveDataSetVariableAsync(string dataSetWriterId,
            string variableId, string generationId,
            CancellationToken ct = default);

        /// <summary>
        /// List all dataset writers or continue find query.
        /// </summary>
        /// <param name="continuation"></param>
        /// <param name="pageSize"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<DataSetWriterInfoListApiModel> ListDataSetWritersAsync(
            string continuation = null, int? pageSize = null,
            CancellationToken ct = default);

        /// <summary>
        /// Find dataset writers for the specified information
        /// criterias.  The returned continuation if any must
        /// be passed to ListDataSetWritersAsync.
        /// </summary>
        /// <param name="query"></param>
        /// <param name="pageSize"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<DataSetWriterInfoListApiModel> QueryDataSetWritersAsync(
            DataSetWriterInfoQueryApiModel query, int? pageSize = null,
            CancellationToken ct = default);

        /// <summary>
        /// Unregister dataset writer and linked items.
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="generationId"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task RemoveDataSetWriterAsync(string dataSetWriterId,
            string generationId, CancellationToken ct = default);

        /// <summary>
        /// Register new writer group
        /// </summary>
        /// <param name="request"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<WriterGroupAddResponseApiModel> AddWriterGroupAsync(
            WriterGroupAddRequestApiModel request,
            CancellationToken ct = default);

        /// <summary>
        /// Read full writer group model which includes all
        /// writers and dataset members if there are any.
        /// </summary>
        /// <param name="writerGroupId">The writerGroupId</param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<WriterGroupApiModel> GetWriterGroupAsync(
            string writerGroupId, CancellationToken ct = default);

        /// <summary>
        /// Activate writer group for publishing
        /// </summary>
        /// <param name="writerGroupId">The writerGroupId</param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task ActivateWriterGroupAsync(string writerGroupId,
            CancellationToken ct = default);

        /// <summary>
        /// Deactivate publishing of network message by writer
        /// group.
        /// </summary>
        /// <param name="writerGroupId">The writerGroupId</param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task DeactivateWriterGroupAsync(string writerGroupId,
            CancellationToken ct = default);

        /// <summary>
        /// Update an existing application, e.g. server
        /// certificate, or additional capabilities.
        /// </summary>
        /// <param name="writerGroupId">The writerGroupId</param>
        /// <param name="request"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task UpdateWriterGroupAsync(string writerGroupId,
            WriterGroupUpdateRequestApiModel request,
            CancellationToken ct = default);

        /// <summary>
        /// List all writer groups or continue find query.
        /// </summary>
        /// <param name="continuation"></param>
        /// <param name="pageSize"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<WriterGroupInfoListApiModel> ListWriterGroupsAsync(
            string continuation = null, int? pageSize = null,
            CancellationToken ct = default);

        /// <summary>
        /// Find writer groups for the specified information
        /// criterias.  The returned continuation if any must
        /// be passed to ListWriterGroupsAsync.
        /// </summary>
        /// <param name="query"></param>
        /// <param name="pageSize"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<WriterGroupInfoListApiModel> QueryWriterGroupsAsync(
            WriterGroupInfoQueryApiModel query, int? pageSize = null,
            CancellationToken ct = default);

        /// <summary>
        /// Unregister writer group and all container writers
        /// and registered variables.
        /// </summary>
        /// <param name="writerGroupId"></param>
        /// <param name="generationId"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task RemoveWriterGroupAsync(string writerGroupId,
            string generationId, CancellationToken ct = default);

        /// <summary>
        /// Add variables to a dataset writer in bulk.
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="request"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<DataSetAddVariableBatchResponseApiModel> AddVariablesToDataSetWriterAsync(
            string dataSetWriterId, DataSetAddVariableBatchRequestApiModel request,
            CancellationToken ct = default);

        /// <summary>
        /// Add variables on endpoint in bulk
        /// </summary>
        /// <param name="endpointId"></param>
        /// <param name="request"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<DataSetAddVariableBatchResponseApiModel> AddVariablesToDefaultDataSetWriterAsync(
            string endpointId, DataSetAddVariableBatchRequestApiModel request,
            CancellationToken ct = default);

        /// <summary>
        /// Remove variables from dataset in bulk
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="request"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<DataSetRemoveVariableBatchResponseApiModel> RemoveVariablesFromDataSetWriterAsync(
            string dataSetWriterId, DataSetRemoveVariableBatchRequestApiModel request,
            CancellationToken ct = default);
    }
}
