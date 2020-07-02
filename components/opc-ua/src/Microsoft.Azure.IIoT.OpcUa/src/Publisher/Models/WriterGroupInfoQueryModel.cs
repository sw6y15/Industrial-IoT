// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Models {

    /// <summary>
    /// Writer group registration query request
    /// </summary>
    public class WriterGroupInfoQueryModel {

        /// <summary>
        /// Return groups with this name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Return only groups in this site
        /// </summary>
        public string SiteId { get; set; }

        /// <summary>
        /// With the specified group version
        /// </summary>
        public uint? GroupVersion { get; set; }

        /// <summary>
        /// And with this message encoding
        /// </summary>
        public NetworkMessageType? MessageType { get; set; }

        /// <summary>
        /// Return groups in the specified state
        /// </summary>
        public WriterGroupState? State { get; set; }

        /// <summary>
        /// Return groups only in the specified state
        /// </summary>
        public byte? Priority { get; set; }
    }
}