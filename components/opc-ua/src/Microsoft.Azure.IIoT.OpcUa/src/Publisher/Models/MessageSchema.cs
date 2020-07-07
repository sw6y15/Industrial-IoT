// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Models {

    /// <summary>
    /// Message schema
    /// </summary>
    public enum MessageSchema {

        /// <summary>
        /// Network messages (default)
        /// </summary>
        PubSub = 1,

        /// <summary>
        /// Monitored item messages
        /// </summary>
        Samples
    }
}