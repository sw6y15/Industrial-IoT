// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher {
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Bulk publish
    /// </summary>
    public interface IBulkPublishHandler {

        /// <summary>
        /// Bulk publish on endpoint
        /// </summary>
        /// <param name="endpointId"></param>
        /// <param name="sessionId"></param>
        /// <param name="nodeset"></param>
        /// <param name="contentType"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task PublishFromNodesetAsync(string endpointId, string sessionId,
            Stream nodeset, string contentType, CancellationToken ct = default);
    }
}