// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Models {
    using System.Runtime.Serialization;

    /// <summary>
    /// Writer group registration query request
    /// </summary>
    [DataContract]
    public class WriterGroupInfoQueryApiModel {

        /// <summary>
        /// Return groups with this name
        /// </summary>
        [DataMember(Name = "name", Order = 0,
            EmitDefaultValue = false)]
        public string Name { get; set; }

        /// <summary>
        /// Return only groups in this site
        /// </summary>
        [DataMember(Name = "siteId", Order = 1,
            EmitDefaultValue = false)]
        public string SiteId { get; set; }

        /// <summary>
        /// With the specified group version
        /// </summary>
        [DataMember(Name = "groupVersion", Order = 2,
            EmitDefaultValue = false)]
        public uint? GroupVersion { get; set; }

        /// <summary>
        /// And with this message encoding
        /// </summary>
        [DataMember(Name = "messageType", Order = 3,
            EmitDefaultValue = false)]
        public NetworkMessageType? MessageType { get; set; }

        /// <summary>
        /// Return groups only in the specified state
        /// </summary>
        [DataMember(Name = "state", Order = 4,
            EmitDefaultValue = false)]
        public WriterGroupState? State { get; set; }

        /// <summary>
        /// Return groups with this priority
        /// </summary>
        [DataMember(Name = "priority", Order = 5,
            EmitDefaultValue = false)]
        public byte? Priority { get; set; }
    }
}