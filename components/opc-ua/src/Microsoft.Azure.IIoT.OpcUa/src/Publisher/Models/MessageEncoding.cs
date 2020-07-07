// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Models {

    /// <summary>
    /// Message encoding
    /// </summary>
    public enum MessageEncoding {

        /// <summary>
        /// Ua Json encoding
        /// </summary>
        Json = 1,

        /// <summary>
        /// Ua binary encoding
        /// </summary>
        Binary,

        /// <summary>
        /// Uadp encoding
        /// </summary>
        Uadp,
    }
}
