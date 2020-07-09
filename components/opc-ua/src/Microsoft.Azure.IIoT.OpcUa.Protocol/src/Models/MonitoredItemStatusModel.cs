// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Protocol.Models {
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;

    /// <summary>
    /// Monitored item status
    /// </summary>
    public class MonitoredItemStatusModel {

        /// <summary>
        /// Identifier of the monitored item
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Monitored item is Event
        /// </summary>
        public bool IsEvent { get; set; }

        /// <summary>
        /// Client handle
        /// </summary>
        public uint? ClientHandle { get; set; }

        /// <summary>
        /// Server identifier if created on server
        /// </summary>
        public uint? ServerId { get; set; }

        /// <summary>
        /// Error information
        /// </summary>
        public ServiceResultModel Error { get; set; }
    }
}