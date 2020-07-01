// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Models {

    /// <summary>
    /// Data set writer query
    /// </summary>
    public class DataSetWriterInfoQueryModel {

        /// <summary>
        /// Query by name
        /// </summary>
        public string DataSetName { get; set; }

        /// <summary>
        /// Query by endpoint
        /// </summary>
        public string EndpointId { get; set; }

        /// <summary>
        /// Dataset writer group.
        /// </summary>
        public string WriterGroupId { get; set; }

        /// <summary>
        /// Whether to exclude disabled items
        /// </summary>
        public bool? ExcludeDisabled { get; set; }
    }
}
