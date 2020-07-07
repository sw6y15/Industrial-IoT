// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------


namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Models {
    using Microsoft.Azure.IIoT.OpcUa.Core;
    using System;

    /// <summary>
    /// Schema type extensions
    /// </summary>
    public static class MessageSchemaEx {

        /// <summary>
        /// Construct message schema from messaging mode and encoding
        /// </summary>
        /// <returns></returns>
        public static string ToMessageSchemaMimeType(this MessageSchema? mode,
            MessageEncoding? encoding) {
            switch (mode) {
                case MessageSchema.Samples:
                    switch (encoding) {
                        case MessageEncoding.Uadp: // Uadp is not supported - assume binary
                        case MessageEncoding.Binary:
                            return MessageSchemaTypes.MonitoredItemMessageBinary;
                        case MessageEncoding.Json:
                        default: // Default encoding is json
                            return MessageSchemaTypes.MonitoredItemMessageJson;
                    }
                case MessageSchema.PubSub:
                default: // Default mode is pub/sub
                    switch (encoding) {
                        case MessageEncoding.Binary:
                        case MessageEncoding.Uadp:
                            return MessageSchemaTypes.NetworkMessageUadp;
                        case MessageEncoding.Json:
                        default: // Default encoding is json
                            return MessageSchemaTypes.NetworkMessageJson;
                    }
            }
        }

        /// <summary>
        /// Get messaging mode and encoding from schema type string
        /// </summary>
        /// <param name="messageSchema"></param>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public static MessageSchema? ParseMessageSchemaMimeType(string messageSchema,
            out MessageEncoding? encoding) {
            switch (messageSchema) {
                case MessageSchemaTypes.MonitoredItemMessageBinary:
                    encoding = MessageEncoding.Binary;
                    return MessageSchema.Samples;
                case MessageSchemaTypes.MonitoredItemMessageJson:
                    encoding = MessageEncoding.Json;
                    return MessageSchema.Samples;
                case MessageSchemaTypes.NetworkMessageUadp:
                    encoding = MessageEncoding.Uadp;
                    return MessageSchema.PubSub;
                case MessageSchemaTypes.NetworkMessageJson:
                    encoding = MessageEncoding.Json;
                    return MessageSchema.PubSub;
                case null:
                    encoding = MessageEncoding.Uadp;
                    return MessageSchema.PubSub;
                default:
                    throw new ArgumentException(nameof(messageSchema),
                        $"Unknown type {messageSchema}");
            }
        }
    }
}