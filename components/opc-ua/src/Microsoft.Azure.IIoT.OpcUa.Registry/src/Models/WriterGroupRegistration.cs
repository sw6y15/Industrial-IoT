// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Registry.Models {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.Hub;
    using System.Runtime.Serialization;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Writer group registration
    /// </summary>
    [DataContract]
    public class WriterGroupRegistration : EntityRegistration {

        /// <inheritdoc/>
        [DataMember]
        public override string DeviceType => IdentityType.WriterGroup;

        /// <summary>
        /// Site of the registration
        /// </summary>
        [DataMember]
        public string SiteId { get; set; }

        /// <summary>
        /// Searchable grouping (either device or site id)
        /// </summary>
        [DataMember]
        public string SiteOrGatewayId =>
            !string.IsNullOrEmpty(SiteId) ? SiteId : DeviceId;

        /// <summary>
        /// Group id
        /// </summary>
        [DataMember]
        public string WriterGroupId { get; set; }

        /// <summary>
        /// Group version
        /// </summary>
        [DataMember]
        public uint? GroupVersion { get; set; }

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
        public Dictionary<string, double> PublishingOffset { get; set; }

        /// <summary>
        /// Locales to use
        /// </summary>
        [DataMember]
        public Dictionary<string, string> LocaleIds { get; set; }

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
        /// Message schema
        /// </summary>
        [DataMember]
        public MessageSchema? Schema { get; set; }

        /// <summary>
        /// Message encoding
        /// </summary>
        [DataMember]
        public MessageEncoding? Encoding { get; set; }

        /// <summary>
        /// Batch buffer size
        /// </summary>
        [DataMember]
        public int? BatchSize { get; set; }

        /// <inheritdoc/>
        public override bool Equals(object obj) {
            if (!(obj is WriterGroupRegistration registration)) {
                return false;
            }
            if (!base.Equals(registration)) {
                return false;
            }
            if (SiteId != registration.SiteId) {
                return false;
            }
            if (Schema != registration.Schema) {
                return false;
            }
            if (Encoding != registration.Encoding) {
                return false;
            }
            if (KeepAliveTime != registration.KeepAliveTime) {
                return false;
            }
            if (PublishingInterval != registration.PublishingInterval) {
                return false;
            }
            if (MaxNetworkMessageSize != registration.MaxNetworkMessageSize) {
                return false;
            }
            if (HeaderLayoutUri != registration.HeaderLayoutUri) {
                return false;
            }
            if (SamplingOffset != registration.SamplingOffset) {
                return false;
            }
            if (DataSetOrdering != registration.DataSetOrdering) {
                return false;
            }
            if (NetworkMessageContentMask != registration.NetworkMessageContentMask) {
                return false;
            }
            if (Priority != registration.Priority) {
                return false;
            }
            if (GroupVersion != registration.GroupVersion) {
                return false;
            }
            if (BatchSize != registration.BatchSize) {
                return false;
            }
            if (WriterGroupId != registration.WriterGroupId) {
                return false;
            }
            if (!LocaleIds.DecodeAsList().SetEqualsSafe(
                    registration.LocaleIds.DecodeAsList(), (x, y) => x == y)) {
                return false;
            }
            if (!PublishingOffset.DecodeAsList().SetEqualsSafe(
                    registration.PublishingOffset.DecodeAsList(), (x, y) => x == y)) {
                return false;
            }
            return true;
        }

        /// <inheritdoc/>
        public static bool operator ==(WriterGroupRegistration r1,
            WriterGroupRegistration r2) =>
            EqualityComparer<WriterGroupRegistration>.Default.Equals(r1, r2);
        /// <inheritdoc/>
        public static bool operator !=(WriterGroupRegistration r1,
            WriterGroupRegistration r2) =>
            !(r1 == r2);

        /// <inheritdoc/>
        public override int GetHashCode() {
            var hashCode = base.GetHashCode();

            hashCode = (hashCode * -1521134295) +
                EqualityComparer<string>.Default.GetHashCode(SiteId);
            hashCode = (hashCode * -1521134295) +
                EqualityComparer<MessageSchema?>.Default.GetHashCode(Schema);
            hashCode = (hashCode * -1521134295) +
                EqualityComparer<MessageEncoding?>.Default.GetHashCode(Encoding);
            hashCode = (hashCode * -1521134295) +
                EqualityComparer<TimeSpan?>.Default.GetHashCode(KeepAliveTime);
            hashCode = (hashCode * -1521134295) +
                EqualityComparer<TimeSpan?>.Default.GetHashCode(PublishingInterval);
            hashCode = (hashCode * -1521134295) +
                EqualityComparer<uint?>.Default.GetHashCode(MaxNetworkMessageSize);
            hashCode = (hashCode * -1521134295) +
                EqualityComparer<string>.Default.GetHashCode(HeaderLayoutUri);
            hashCode = (hashCode * -1521134295) +
                EqualityComparer<string>.Default.GetHashCode(WriterGroupId);
            hashCode = (hashCode * -1521134295) +
                EqualityComparer<double?>.Default.GetHashCode(SamplingOffset);
            hashCode = (hashCode * -1521134295) +
                EqualityComparer<DataSetOrderingType?>.Default.GetHashCode(DataSetOrdering);
            hashCode = (hashCode * -1521134295) +
                EqualityComparer<NetworkMessageContentMask?>.Default.GetHashCode(NetworkMessageContentMask);
            hashCode = (hashCode * -1521134295) +
                EqualityComparer<byte?>.Default.GetHashCode(Priority);
            hashCode = (hashCode * -1521134295) +
                EqualityComparer<uint?>.Default.GetHashCode(GroupVersion);
            hashCode = (hashCode * -1521134295) +
                EqualityComparer<int?>.Default.GetHashCode(BatchSize);
            return hashCode;
        }

        internal bool IsInSync() {
            return _isInSync;
        }

        internal bool _isInSync;
    }
}