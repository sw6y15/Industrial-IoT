// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Writer group registry extensions
    /// </summary>
    public static class WriterGroupRegistryEx {

        /// <summary>
        /// Find writer groups
        /// </summary>
        /// <param name="service"></param>
        /// <param name="query"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public static async Task<List<WriterGroupInfoModel>> QueryAllWriterGroupsAsync(
            this IWriterGroupRegistry service, WriterGroupInfoQueryModel query,
            CancellationToken ct = default) {
            var registrations = new List<WriterGroupInfoModel>();
            var result = await service.QueryWriterGroupsAsync(query, null, ct);
            registrations.AddRange(result.WriterGroups);
            while (result.ContinuationToken != null) {
                result = await service.ListWriterGroupsAsync(result.ContinuationToken,
                    null, ct);
                registrations.AddRange(result.WriterGroups);
            }
            return registrations;
        }

        /// <summary>
        /// List all writer groups
        /// </summary>
        /// <param name="service"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public static async Task<List<WriterGroupInfoModel>> ListAllWriterGroupsAsync(
            this IWriterGroupRegistry service, CancellationToken ct = default) {
            var registrations = new List<WriterGroupInfoModel>();
            var result = await service.ListWriterGroupsAsync(null, null, ct);
            registrations.AddRange(result.WriterGroups);
            while (result.ContinuationToken != null) {
                result = await service.ListWriterGroupsAsync(result.ContinuationToken,
                    null, ct);
                registrations.AddRange(result.WriterGroups);
            }
            return registrations;
        }
    }
}
