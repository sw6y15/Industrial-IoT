// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher {
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Emits network messages for the writer group.
    /// </summary>
    public interface IWriterGroupMessageEmitter {

        /// <summary>
        /// Dataset writer group identifier
        /// </summary>
        string WriterGroupId { get; set; }

        /// <summary>
        /// Network message encoding to generate
        /// </summary>
        MessageEncoding? Encoding { get; set; }

        /// <summary>
        /// The message schema to use
        /// </summary>
        MessageSchema? Schema { get; set; }

        /// <summary>
        /// Network message content
        /// </summary>
        NetworkMessageContentMask? MessageContentMask { get; set; }

        /// <summary>
        /// Max network message size
        /// </summary>
        uint? MaxNetworkMessageSize { get; set; }

        /// <summary>
        /// Header layout uri
        /// </summary>
        string HeaderLayoutUri { get; set; }

        /// <summary>
        /// Batch buffer size (Publisher extension)
        /// </summary>
        int? BatchSize { get; set; }

        /// <summary>
        /// Publishing interval
        /// </summary>
        TimeSpan? PublishingInterval { get; set; }

        /// <summary>
        /// Uadp dataset ordering
        /// </summary>
        DataSetOrderingType? DataSetOrdering { get; set; }

        /// <summary>
        /// Publishing offset for uadp messages
        /// </summary>
        List<double> PublishingOffset { get; set; }

        /// <summary>
        /// Enqueue new message for processing - will block
        /// in case of backlog.
        /// </summary>
        /// <param name="message"></param>
        void Enqueue(DataSetWriterMessageModel message);
    }
}