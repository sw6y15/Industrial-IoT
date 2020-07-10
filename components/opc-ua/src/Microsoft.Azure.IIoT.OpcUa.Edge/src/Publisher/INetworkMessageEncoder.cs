// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher {
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using System.Collections.Generic;

    /// <summary>
    /// Encoder to encode data set writer messages
    /// </summary>
    public interface INetworkMessageEncoder {

        /// <summary>
        /// Messaging schema the encoder produces
        /// </summary>
        string MessageSchema { get; }

        /// <summary>
        /// Number of notifications that are too big to be processed to IotHub Messages
        /// </summary>
        uint NotificationsDroppedCount { get; }

        /// <summary>
        /// Number of successfully processed notifications from OPC client
        /// </summary>
        uint NotificationsProcessedCount { get; }

        /// <summary>
        /// Number of successfully processed messages
        /// </summary>
        uint MessagesProcessedCount { get; }

        /// <summary>
        /// Average notifications in a message
        /// </summary>
        double AvgNotificationsPerMessage { get; }

        /// <summary>
        /// Average notifications in a message
        /// </summary>
        double AvgMessageSize { get; }

        /// <summary>
        /// Encodes the list of messages into single message NetworkMessageModel list
        /// </summary>
        /// <param name="writerGroupId"></param>
        /// <param name="message"></param>
        /// <param name="headerLayoutUri"></param>
        /// <param name="contentMask"></param>
        /// <param name="order"></param>
        /// <param name="maxMessageSize"></param>
        /// <returns></returns>
        IEnumerable<NetworkMessageModel> Encode(string writerGroupId,
            IEnumerable<DataSetWriterMessageModel> message,
            string headerLayoutUri, NetworkMessageContentMask? contentMask,
            DataSetOrderingType? order, int maxMessageSize);

        /// <summary>
        /// Encodes the list of messages into batched message list
        /// </summary>
        /// <param name="writerGroupId"></param>
        /// <param name="headerLayoutUri"></param>
        /// <param name="contentMask"></param>
        /// <param name="order"></param>
        /// <param name="messages"></param>
        /// <param name="maxMessageSize"></param>
        /// <returns></returns>
        IEnumerable<NetworkMessageModel> EncodeBatch(string writerGroupId,
            IEnumerable<DataSetWriterMessageModel> messages,
            string headerLayoutUri, NetworkMessageContentMask? contentMask,
            DataSetOrderingType? order, int maxMessageSize);
    }
}