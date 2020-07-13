// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Models {

    /// <summary>
    /// Dataset writer model extensions
    /// </summary>
    public static class DataSetWriterModelEx {

        /// <summary>
        /// Clone
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public static DataSetWriterModel Clone(this DataSetWriterModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterModel {
                DataSet = model.DataSet.Clone(),
                DataSetFieldContentMask = model.DataSetFieldContentMask,
                DataSetMetaDataSendInterval = model.DataSetMetaDataSendInterval,
                DataSetWriterId = model.DataSetWriterId,
                KeyFrameCount = model.KeyFrameCount,
                GenerationId = model.GenerationId,
                KeyFrameInterval = model.KeyFrameInterval,
                MessageSettings = model.MessageSettings.Clone()
            };
        }

        /// <summary>
        /// Convert to info model
        /// </summary>
        /// <param name="model"></param>
        /// <param name="endpointId"></param>
        /// <param name="writerGroupId"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public static DataSetWriterInfoModel AsDataSetWriterInfo(
            this DataSetWriterModel model, string writerGroupId, string endpointId,
            PublisherOperationContextModel context) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterInfoModel {
                DataSet = model.DataSet.AsPublishedDataSetSourceInfo(endpointId),
                WriterGroupId = writerGroupId,
                IsDisabled = false,
                Created = context,
                Updated = context,
                DataSetFieldContentMask = model.DataSetFieldContentMask,
                DataSetMetaDataSendInterval = model.DataSetMetaDataSendInterval,
                DataSetWriterId = model.DataSetWriterId,
                KeyFrameCount = model.KeyFrameCount,
                GenerationId = model.GenerationId,
                KeyFrameInterval = model.KeyFrameInterval,
                MessageSettings = model.MessageSettings.Clone()
            };
        }
    }
}