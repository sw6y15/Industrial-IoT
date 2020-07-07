// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Models {
    using System.Runtime.Serialization;

    /// <summary>
    /// Message encoding
    /// </summary>
    [DataContract]
    public enum MessageEncoding {

        /// <summary>
        /// Ua Json encoding
        /// </summary>
        [EnumMember]
        Json = 1,

        /// <summary>
        /// Ua binary encoding
        /// </summary>
        [EnumMember]
        Binary,

        /// <summary>
        /// Uadp encoding
        /// </summary>
        [EnumMember]
        Uadp,
    }
}
