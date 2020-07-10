// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Registry.Models {
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;
    using System;

    /// <summary>
    /// Dataset writer group state change
    /// </summary>
    public class WriterGroupStateEventModel {

        /// <summary>
        /// Writer group
        /// </summary>
        public string WriterGroupId { get; set; }

        /// <summary>
        /// Dataset writer id if dataset related or null
        /// </summary>
        public string DataSetWriterId { get; set; }

        /// <summary>
        /// Variable id if variable related or null
        /// </summary>
        public string PublishedVariableId { get; set; }

        /// <summary>
        /// Type of event
        /// </summary>
        public PublisherStateEventType EventType { get; set; }

        /// <summary>
        /// Timestamp of event
        /// </summary>
        public DateTime TimeStamp { get; set; }

        /// <summary>
        /// Result if event is result of service call
        /// </summary>
        public ServiceResultModel LastResult { get; set; }

        /// <summary>
        /// Endpoint state if state changed
        /// </summary>
        public EndpointConnectivityState? EndpointState { get; set; }
    }
}
