// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Models {
    using System.Runtime.Serialization;
    using System;

    /// <summary>
    /// State of the writer group
    /// </summary>
    [DataContract]
    public class WriterGroupStateApiModel {

        /// <summary>
        /// State indicator
        /// </summary>
        [DataMember(Name = "state", Order = 0,
            EmitDefaultValue = false)]
        public WriterGroupState State { get; set; }

        /// <summary>
        /// Last state change
        /// </summary>
        [DataMember(Name = "lastStateChange", Order = 1,
            EmitDefaultValue = false)]
        public DateTime LastStateChange { get; set; }
    }
}