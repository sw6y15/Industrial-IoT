// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using System.Threading.Tasks;

    /// <summary>
    /// Notified when writers change
    /// </summary>
    public interface IDataSetWriterRegistryListener {

        /// <summary>
        /// New dataset writer
        /// </summary>
        /// <param name="context"></param>
        /// <param name="dataSetWriter"></param>
        /// <returns></returns>
        Task OnDataSetWriterAddedAsync(PublisherOperationContextModel context,
            DataSetWriterInfoModel dataSetWriter);

        /// <summary>
        /// Called when writer was updated
        /// </summary>
        /// <param name="context"></param>
        /// <param name="dataSetWriterId"></param>
        /// <param name="dataSetWriter"></param>
        /// <returns></returns>
        Task OnDataSetWriterUpdatedAsync(PublisherOperationContextModel context,
            string dataSetWriterId, DataSetWriterInfoModel dataSetWriter = null);

        /// <summary>
        /// Called when writer state changed
        /// </summary>
        /// <param name="context"></param>
        /// <param name="dataSetWriterId"></param>
        /// <param name="dataSetWriter"></param>
        /// <returns></returns>
        Task OnDataSetWriterStateChangeAsync(PublisherOperationContextModel context,
            string dataSetWriterId, DataSetWriterInfoModel dataSetWriter = null);

        /// <summary>
        /// Called when writer was deleted which implies all
        /// dataset items were also deleted.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="dataSetWriter"></param>
        /// <returns></returns>
        Task OnDataSetWriterRemovedAsync(PublisherOperationContextModel context,
            DataSetWriterInfoModel dataSetWriter);
    }
}
