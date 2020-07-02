// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Writer group registry
    /// </summary>
    public interface IWriterGroupRegistry {

        /// <summary>
        /// Register new writer group
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<WriterGroupAddResultModel> AddWriterGroupAsync(
            WriterGroupAddRequestModel request,
            PublisherOperationContextModel context = null,
            CancellationToken ct = default);

        /// <summary>
        /// Read full writer group model which includes all
        /// writers and dataset members if there are any.
        /// </summary>
        /// <param name="writerGroupId">The writer group</param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<WriterGroupModel> GetWriterGroupAsync(
            string writerGroupId, CancellationToken ct = default);

        /// <summary>
        /// Update an existing application, e.g. server
        /// certificate, or additional capabilities.
        /// </summary>
        /// <param name="writerGroupId">The writer group</param>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task UpdateWriterGroupAsync(string writerGroupId,
            WriterGroupUpdateRequestModel request,
            PublisherOperationContextModel context = null,
            CancellationToken ct = default);

        /// <summary>
        /// Activate a writer group setting the state to pending.
        /// </summary>
        /// <param name="writerGroupId">The writer group</param>
        /// <param name="context"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task ActivateWriterGroupAsync(string writerGroupId,
            PublisherOperationContextModel context = null,
            CancellationToken ct = default);

        /// <summary>
        /// List all writer groups or continue find query.
        /// </summary>
        /// <param name="continuation"></param>
        /// <param name="pageSize"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task<WriterGroupInfoListModel> ListWriterGroupsAsync(
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
        Task<WriterGroupInfoListModel> QueryWriterGroupsAsync(
            WriterGroupInfoQueryModel query, int? pageSize = null,
            CancellationToken ct = default);

        /// <summary>
        /// Suspend the writer group operation.
        /// </summary>
        /// <param name="writerGroupId">The writer group</param>
        /// <param name="context"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task DeactivateWriterGroupAsync(string writerGroupId,
            PublisherOperationContextModel context = null,
            CancellationToken ct = default);

        /// <summary>
        /// Unregister writer group and all container writers
        /// and registered variables.
        /// </summary>
        /// <param name="writerGroupId"></param>
        /// <param name="generationId"></param>
        /// <param name="context"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task RemoveWriterGroupAsync(string writerGroupId, string generationId,
            PublisherOperationContextModel context = null,
            CancellationToken ct = default);
    }
}
