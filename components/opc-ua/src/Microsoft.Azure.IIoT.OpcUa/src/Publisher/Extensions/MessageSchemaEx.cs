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
        /// Match to message schema
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="mimeType"></param>
        /// <returns></returns>
        public static bool Matches(this MessageSchema schema, string mimeType) {
            switch (mimeType) {
                case MessageSchemaTypes.MonitoredItemMessageBinary:
                case MessageSchemaTypes.MonitoredItemMessageJson:
                    return schema == MessageSchema.Samples;
                case MessageSchemaTypes.NetworkMessageUadp:
                case MessageSchemaTypes.NetworkMessageJson:
                    return schema == MessageSchema.PubSub;
                case null:
                default:
                    throw new ArgumentException(nameof(mimeType),
                        $"Unknown type {mimeType}");
            }
        }

        /// <summary>
        /// Match encoding
        /// </summary>
        /// <param name="mimeType"></param>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public static bool Matches(this MessageEncoding encoding, string mimeType) {
            switch (mimeType) {
                case MessageSchemaTypes.NetworkMessageUadp:
                case MessageSchemaTypes.MonitoredItemMessageBinary:
                    return
                        encoding == MessageEncoding.Uadp ||
                        encoding == MessageEncoding.Binary;
                case MessageSchemaTypes.NetworkMessageJson:
                case MessageSchemaTypes.MonitoredItemMessageJson:
                    return encoding == MessageEncoding.Json;
                default:
                    throw new ArgumentException(nameof(mimeType),
                        $"Unknown type {mimeType}");
            }
        }

        /// <summary>
        /// Match content mask
        /// </summary>
        /// <param name="mimeType"></param>
        /// <param name="content"></param>
        /// <returns></returns>
        public static bool Matches(this NetworkMessageContentMask content, string mimeType) {
            var isNetworkMessage = !content.HasFlag(NetworkMessageContentMask.NetworkMessageHeader);
            var isDataSetMessage = !content.HasFlag(NetworkMessageContentMask.DataSetMessageHeader);
            switch (mimeType) {
                case MessageSchemaTypes.NetworkMessageUadp:
                case MessageSchemaTypes.MonitoredItemMessageBinary:
                case MessageSchemaTypes.NetworkMessageJson:
                case MessageSchemaTypes.MonitoredItemMessageJson:
                    return true; // TODO -
                default:
                    throw new ArgumentException(nameof(mimeType),
                        $"Unknown type {mimeType}");
            }
        }
    }
}