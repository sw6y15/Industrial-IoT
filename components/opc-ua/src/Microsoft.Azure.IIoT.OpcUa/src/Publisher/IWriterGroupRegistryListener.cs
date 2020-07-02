// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using System.Collections;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Notified when writer groups change
    /// </summary>
    public interface IWriterGroupRegistryListener {

        /// <summary>
        /// New group added
        /// </summary>
        /// <param name="context"></param>
        /// <param name="writerGroup"></param>
        /// <returns></returns>
        Task OnWriterGroupAddedAsync(PublisherOperationContextModel context,
            WriterGroupInfoModel writerGroup);

        /// <summary>
        /// Called when group or group content was updated
        /// </summary>
        /// <param name="context"></param>
        /// <param name="writerGroup"></param>
        /// <returns></returns>
        Task OnWriterGroupUpdatedAsync(PublisherOperationContextModel context,
            WriterGroupInfoModel writerGroup);

        /// <summary>
        /// Called when group state changed
        /// </summary>
        /// <param name="context"></param>
        /// <param name="writerGroup"></param>
        /// <returns></returns>
        Task OnWriterGroupStateChangeAsync(PublisherOperationContextModel context,
            WriterGroupInfoModel writerGroup);

        /// <summary>
        /// Called when group was activated
        /// </summary>
        /// <param name="context"></param>
        /// <param name="writerGroup"></param>
        /// <returns></returns>
        Task OnWriterGroupActivatedAsync(PublisherOperationContextModel context,
            WriterGroupInfoModel writerGroup);

        /// <summary>
        /// Called when group was deactivated
        /// </summary>
        /// <param name="context"></param>
        /// <param name="writerGroup"></param>
        /// <returns></returns>
        Task OnWriterGroupDeactivatedAsync(PublisherOperationContextModel context,
            WriterGroupInfoModel writerGroup);

        /// <summary>
        /// Called when writer group.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="writerGroupId"></param>
        /// <returns></returns>
        Task OnWriterGroupRemovedAsync(PublisherOperationContextModel context,
            string writerGroupId);
    }
}
