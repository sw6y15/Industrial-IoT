// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Writer group bulk operations
    /// </summary>
    public interface IWriterGroupBatchOperations {

        /// <summary>
        /// Import a writer group
        /// </summary>
        /// <param name="writerGroup"></param>
        /// <param name="context"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        Task ImportWriterGroupAsync(WriterGroupModel writerGroup,
            PublisherOperationContextModel context = null,
            CancellationToken ct = default);
    }
}
