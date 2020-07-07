// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Models {
    using System.Runtime.Serialization;

    /// <summary>
    /// Message schema
    /// </summary>
    [DataContract]
    public enum MessageSchema {

        /// <summary>
        /// Network messages (default)
        /// </summary>
        [EnumMember]
        PubSub = 1,

        /// <summary>
        /// Monitored item samples
        /// </summary>
        [EnumMember]
        Samples
    }
}