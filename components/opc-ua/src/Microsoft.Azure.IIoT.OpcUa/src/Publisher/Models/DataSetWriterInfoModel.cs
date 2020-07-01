// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Models {
    using System;

    /// <summary>
    /// Data set writer
    /// </summary>
    public class DataSetWriterInfoModel {

        /// <summary>
        /// Dataset writer id
        /// </summary>
        public string DataSetWriterId { get; set; }

        /// <summary>
        /// Dataset writer group the writer is part of or default
        /// </summary>
        public string WriterGroupId { get; set; }

        /// <summary>
        /// Generation id
        /// </summary>
        public string GenerationId { get; set; }

        /// <summary>
        /// Dataset information
        /// </summary>
        public PublishedDataSetSourceInfoModel DataSet { get; set; }

        /// <summary>
        /// Dataset field content mask the writer applies
        /// </summary>
        public DataSetFieldContentMask? DataSetFieldContentMask { get; set; }

        /// <summary>
        /// Data set message settings
        /// </summary>
        public DataSetWriterMessageSettingsModel MessageSettings { get; set; }

        /// <summary>
        /// Keyframe count
        /// </summary>
        public uint? KeyFrameCount { get; set; }

        /// <summary>
        /// Or keyframe timer interval (publisher extension)
        /// </summary>
        public TimeSpan? KeyFrameInterval { get; set; }

        /// <summary>
        /// Metadata message sending interval (publisher extension)
        /// </summary>
        public TimeSpan? DataSetMetaDataSendInterval { get; set; }

        /// <summary>
        /// Dataset writer is disabled
        /// </summary>
        public bool? IsDisabled { get; set; }

        /// <summary>
        /// Last updated
        /// </summary>
        public PublisherOperationContextModel Updated { get; set; }

        /// <summary>
        /// Created
        /// </summary>
        public PublisherOperationContextModel Created { get; set; }
    }
}
