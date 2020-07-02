// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Events.v2.Models {

    /// <summary>
    /// Writer event type
    /// </summary>
    public enum DataSetWriterEventType {

        /// <summary>
        /// New
        /// </summary>
        Added,

        /// <summary>
        /// Updated
        /// </summary>
        Updated,

        /// <summary>
        /// Activated
        /// </summary>
        Activated,

        /// <summary>
        /// Deactivated
        /// </summary>
        Deactivated,

        /// <summary>
        /// State change
        /// </summary>
        StateChange,

        /// <summary>
        /// Deleted
        /// </summary>
        Removed,
    }
}