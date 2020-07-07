// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Storage.Default {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using System.Runtime.Serialization;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Writer group document model
    /// </summary>
    [DataContract]
    public class WriterGroupDocument {

        /// <summary>
        /// Document type
        /// </summary>
        [DataMember]
        public string ClassType { get; set; } = ClassTypeName;
        /// <summary/>
        public static readonly string ClassTypeName = "WriterGroup";

        /// <summary>
        /// Identifier of the document
        /// </summary>
        [DataMember(Name = "id")]
        public string Id { get; set; }

        /// <summary>
        /// Group id
        /// </summary>
        [DataMember]
        public string WriterGroupId => Id;

        /// <summary>
        /// Name of the writer group
        /// </summary>
        [DataMember]
        public string Name { get; set; }

        /// <summary>
        /// Group version
        /// </summary>
        [DataMember]
        public uint? GroupVersion { get; set; }

        /// <summary>
        /// Site the group can be applied to
        /// </summary>
        [DataMember]
        public string SiteId { get; set; }

        /// <summary>
        /// Priority of the writer group
        /// </summary>
        [DataMember]
        public byte? Priority { get; set; }

        /// <summary>
        /// Network message content
        /// </summary>
        [DataMember]
        public NetworkMessageContentMask? NetworkMessageContentMask { get; set; }

        /// <summary>
        /// Uadp dataset ordering
        /// </summary>
        [DataMember]
        public DataSetOrderingType? DataSetOrdering { get; set; }

        /// <summary>
        /// Uadp Sampling offset
        /// </summary>
        [DataMember]
        public double? SamplingOffset { get; set; }

        /// <summary>
        /// Publishing offset for uadp messages
        /// </summary>
        [DataMember]
        public List<double> PublishingOffset { get; set; }

        /// <summary>
        /// Locales to use
        /// </summary>
        [DataMember]
        public List<string> LocaleIds { get; set; }

        /// <summary>
        /// Header layout uri
        /// </summary>
        [DataMember]
        public string HeaderLayoutUri { get; set; }

        /// <summary>
        /// Max network message size
        /// </summary>
        [DataMember]
        public uint? MaxNetworkMessageSize { get; set; }

        /// <summary>
        /// Publishing interval
        /// </summary>
        [DataMember]
        public TimeSpan? PublishingInterval { get; set; }

        /// <summary>
        /// Keep alive time
        /// </summary>
        [DataMember]
        public TimeSpan? KeepAliveTime { get; set; }

        /// <summary>
        /// Encoding to generate
        /// </summary>
        [DataMember]
        public MessageEncoding? MessageEncoding { get; set; }

        /// <summary>
        /// The messaging mode for outgoing messages.
        /// </summary>
        [DataMember]
        public MessageSchema? Schema { get; set; }

        /// <summary>
        /// Batch buffer size
        /// </summary>
        [DataMember]
        public int? BatchSize { get; set; }

        /// <summary>
        /// Last reported state
        /// </summary>
        [DataMember]
        public WriterGroupState LastState { get; set; }

        /// <summary>
        /// Last state change
        /// </summary>
        [DataMember]
        public DateTime LastStateChange { get; set; }

        /// <summary>
        /// Updated at
        /// </summary>
        [DataMember]
        public DateTime? Updated { get; set; }

        /// <summary>
        /// Updated authority
        /// </summary>
        [DataMember]
        public string UpdatedAuditId { get; set; }

        /// <summary>
        /// Created at
        /// </summary>
        [DataMember]
        public DateTime? Created { get; set; }

        /// <summary>
        /// Created authority
        /// </summary>
        [DataMember]
        public string CreatedAuditId { get; set; }

        /// <summary>
        /// Etag
        /// </summary>
        [DataMember(Name = "_etag")]
        public string ETag { get; set; }
    }
}