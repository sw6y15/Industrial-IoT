// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Models {
    using Microsoft.Azure.IIoT.OpcUa.Api.Core.Models;
    using System.Runtime.Serialization;
    using System;

    /// <summary>
    /// State of the dataset writer source
    /// </summary>
    [DataContract]
    public class PublishedDataSetSourceStateApiModel {

        /// <summary>
        /// Current endpoint state
        /// </summary>
        [DataMember(Name = "endpointState", Order = 0,
            EmitDefaultValue = false)]
        public EndpointConnectivityState? EndpointState { get; set; }

        /// <summary>
        /// Last operation result
        /// </summary>
        [DataMember(Name = "lastResult", Order = 1,
            EmitDefaultValue = false)]
        public ServiceResultApiModel LastResult { get; set; }

        /// <summary>
        /// Last result change
        /// </summary>
        [DataMember(Name = "lastResultChange", Order = 2,
            EmitDefaultValue = false)]
        public DateTime? LastResultChange { get; set; }
    }
}