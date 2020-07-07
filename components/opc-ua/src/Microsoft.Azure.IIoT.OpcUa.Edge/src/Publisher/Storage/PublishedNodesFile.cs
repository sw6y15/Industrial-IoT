// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Services {
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Protocol.Models;
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;
    using Microsoft.Azure.IIoT.OpcUa.Core;
    using Microsoft.Azure.IIoT.Crypto;
    using Microsoft.Azure.IIoT.Serializers;
    using Serilog;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Diagnostics;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// Loads published nodes file and configures the engine
    /// </summary>
    public class PublishedNodesFile {

        /// <summary>
        /// Name of the file
        /// </summary>
        public string FileName { get; }

        /// <summary>
        /// Create published nodes file loader
        /// </summary>
        /// <param name="serializer"></param>
        /// <param name="fileName"></param>
        /// <param name="logger"></param>
        /// <param name="cryptoProvider"></param>
        public PublishedNodesFile(string fileName, IJsonSerializer serializer,
            ILogger logger, ISecureElement cryptoProvider = null) :
            this (serializer, new LegacyCliModel {
                PublishedNodesFile = fileName
            }, logger, cryptoProvider) {
        }

        /// <summary>
        /// Create published nodes file loader
        /// </summary>
        /// <param name="serializer"></param>
        /// <param name="legacyCliModelProvider"></param>
        /// <param name="logger"></param>
        /// <param name="cryptoProvider"></param>
        public PublishedNodesFile(IJsonSerializer serializer,
            ILegacyCliModelProvider legacyCliModelProvider, ILogger logger,
            ISecureElement cryptoProvider = null) {

            _legacyCliModel = legacyCliModelProvider?.LegacyCliModel ??
                throw new ArgumentNullException(nameof(legacyCliModelProvider));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _cryptoProvider = cryptoProvider;

            FileName = _legacyCliModel.PublishedNodesFile;
        }

        /// <summary>
        /// Read writer group from file
        /// </summary>
        /// <returns></returns>
        public WriterGroupModel Read() {
            using (var stream = File.OpenRead(FileName))
            using (var reader = new StreamReader(stream)) {
                return Read(reader);
            }
        }

        /// <summary>
        /// Read writer group from reader
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        public WriterGroupModel Read(TextReader reader) {
            var sw = Stopwatch.StartNew();
            _logger.Debug("Reading published nodes file ({elapsed}", sw.Elapsed);
            var items = _serializer.Deserialize<List<PublishedNodesEntryModel>>(
                reader);
            _logger.Information(
                "Read {count} items from published nodes file in {elapsed}",
                items.Count, sw.Elapsed);
            sw.Restart();

            var writerGroup = new WriterGroupModel {
                Encoding = _legacyCliModel.Encoding,
                Schema = _legacyCliModel.Schema,
                PublishingInterval = _legacyCliModel.BatchTriggerInterval,
                BatchSize = _legacyCliModel.BatchSize,
                MaxNetworkMessageSize = _legacyCliModel.MaxMessageSize,
                DataSetWriters = ToDataSetWriters(items, _legacyCliModel).ToList(),
                MessageSettings = new WriterGroupMessageSettingsModel {
                    NetworkMessageContentMask =
                        NetworkMessageContentMask.PublisherId |
                        NetworkMessageContentMask.WriterGroupId |
                        NetworkMessageContentMask.NetworkMessageNumber |
                        NetworkMessageContentMask.SequenceNumber |
                        NetworkMessageContentMask.PayloadHeader |
                        NetworkMessageContentMask.Timestamp |
                        NetworkMessageContentMask.DataSetClassId |
                        NetworkMessageContentMask.NetworkMessageHeader |
                        NetworkMessageContentMask.DataSetMessageHeader
                }
            };
            _logger.Information("Converted items to jobs in {elapsed}", sw.Elapsed);
            return writerGroup;
        }

        /// <summary>
        /// Write items to file
        /// </summary>
        /// <param name="writerGroup"></param>
        /// <returns></returns>
        public void Write(WriterGroupModel writerGroup) {
            using (var stream = File.OpenWrite(FileName))
            using (var writer = new StreamWriter(stream)) {
                Write(writerGroup, writer);
            }
        }

        /// <summary>
        /// Write items to writer
        /// </summary>
        /// <param name="writerGroup"></param>
        /// <param name="writer"></param>
        /// <returns></returns>
        public void Write(WriterGroupModel writerGroup, TextWriter writer) {
            var sw = Stopwatch.StartNew();
            var nodes = FromDataSetWriters(writerGroup.DataSetWriters);
            _logger.Information("Converted writer group to published nodes in {elapsed}",
                sw.Elapsed);
            sw.Restart();
            _logger.Debug("Writing published nodes file ({elapsed}", sw.Elapsed);
            var items = _serializer.SerializeToString(nodes);
            writer.Write(items);
            _logger.Information(
                "Wrote {count} items to published nodes file in {elapsed}",
                    nodes.Count(), sw.Elapsed);
            sw.Restart();
        }

        /// <summary>
        /// Convert data set writers to nodes
        /// </summary>
        /// <param name="dataSetWriters"></param>
        /// <returns></returns>
        private IEnumerable<PublishedNodesEntryModel> FromDataSetWriters(
            List<DataSetWriterModel> dataSetWriters) {
            if (dataSetWriters == null) {
                return Enumerable.Empty<PublishedNodesEntryModel>();
            }
            try {
                return dataSetWriters
                    .Where(writer =>
                        writer?.DataSet?.DataSetSource?.Connection?.Endpoint?.Url != null)
                    .Where(writer =>
                        writer.DataSet.DataSetSource.PublishedVariables?.PublishedData?.Any() ?? false)
                    .Select(writer => new PublishedNodesEntryModel {
                        EndpointUrl =
                            new Uri(writer.DataSet.DataSetSource.Connection.Endpoint.Url),
                        UseSecurity =
                            writer.DataSet.DataSetSource.Connection.Endpoint.SecurityMode
                                != SecurityMode.None,
                        OpcAuthenticationMode = ToUserNamePasswordCredential(
                            writer.DataSet.DataSetSource.Connection.User, out var user, out var password),
                        OpcAuthenticationUsername = user,
                        OpcAuthenticationPassword = password,
                        OpcNodes = writer.DataSet.DataSetSource.PublishedVariables.PublishedData
                            .Select(v => new OpcNodeModel {
                                DisplayName = v.PublishedVariableDisplayName,
                                ExpandedNodeId = v.PublishedVariableNodeId,
                                HeartbeatIntervalTimespan = v.HeartbeatInterval,
                                OpcSamplingIntervalTimespan = v.SamplingInterval,
                                OpcPublishingIntervalTimespan =
                                    writer.DataSet.DataSetSource.SubscriptionSettings?.PublishingInterval,
                                SkipFirst = null, // TODO
                                Id = v.Id
                            })
                            .ToList()
                    });
            }
            catch (Exception ex) {
                _logger.Error(ex, "failed to convert writers to published nodes.");
                return Enumerable.Empty<PublishedNodesEntryModel>();
            }
        }

        /// <summary>
        /// Convert nodes to writers
        /// </summary>
        /// <param name="items"></param>
        /// <param name="legacyCliModel">The legacy command line arguments</param>
        /// <returns></returns>
        private IEnumerable<DataSetWriterModel> ToDataSetWriters(
            IEnumerable<PublishedNodesEntryModel> items, LegacyCliModel legacyCliModel) {
            if (items == null) {
                return Enumerable.Empty<DataSetWriterModel>();
            }
            try {
                return items
                    // Group by connection
                    .GroupBy(item => new ConnectionModel {
                        OperationTimeout = legacyCliModel.OperationTimeout,
                        Endpoint = new EndpointModel {
                            Url = item.EndpointUrl.OriginalString,
                            SecurityMode = item.UseSecurity == false &&
                                item.OpcAuthenticationMode != OpcAuthenticationMode.UsernamePassword ?
                                    SecurityMode.None : SecurityMode.Best
                        },
                        User = item.OpcAuthenticationMode != OpcAuthenticationMode.UsernamePassword ?
                                null : ToUserNamePasswordCredentialAsync(item).Result
                        },
                        // Select and batch nodes into published data set sources
                        item => GetNodeModels(item, legacyCliModel.ScaleTestCount.GetValueOrDefault(1)),
                        // Comparer for connection information
                        new FuncCompare<ConnectionModel>((x, y) => x.IsSameAs(y)))
                    .Select(group => group
                        // Flatten all nodes for the same connection and group by publishing interval
                        // then batch in chunks for max 1000 nodes and create data sets from those.
                        .Flatten()
                        .GroupBy(n => n.OpcPublishingInterval)
                        .SelectMany(n => n
                            .Distinct((a, b) => a.Id == b.Id && a.DisplayName == b.DisplayName &&
                                        a.OpcSamplingInterval == b.OpcSamplingInterval)
                            .Batch(1000))
                        .Select(opcNodes => new PublishedDataSetSourceModel {
                            Connection = group.Key.Clone(),
                            SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                                PublishingInterval = GetPublishingIntervalFromNodes(opcNodes, legacyCliModel),
                                ResolveDisplayName = legacyCliModel.FetchOpcNodeDisplayName
                            },
                            PublishedVariables = new PublishedDataItemsModel {
                                PublishedData = opcNodes
                                    .Select(node => new PublishedDataSetVariableModel {
                                        // this is the monitored item id, not the nodeId!
                                        // Use the display name if any otherwisw the nodeId
                                        Id = string.IsNullOrEmpty(node.DisplayName)
                                            ? node.Id : node.DisplayName,
                                        PublishedVariableNodeId = node.Id,
                                        PublishedVariableDisplayName = node.DisplayName,
                                        SamplingInterval = node.OpcSamplingIntervalTimespan ??
                                            legacyCliModel.DefaultSamplingInterval,
                                        HeartbeatInterval = node.HeartbeatInterval.HasValue ?
                                            TimeSpan.FromSeconds(node.HeartbeatInterval.Value) :
                                            legacyCliModel.DefaultHeartbeatInterval,
                                        QueueSize = legacyCliModel.DefaultQueueSize,
                                        // TODO: skip first?
                                        // SkipFirst = opcNode.SkipFirst,
                                    }).ToList()
                            }
                        }))
                    .SelectMany(dataSetSourceBatches => dataSetSourceBatches
                        .Select(dataSetSource => new DataSetWriterModel {
                            DataSetWriterId = dataSetSource.Connection.Endpoint.Url + "_" + Hash(dataSetSource),
                            DataSet = new PublishedDataSetModel {
                                DataSetSource = dataSetSource.Clone(),
                            },
                            DataSetFieldContentMask =
                                DataSetFieldContentMask.StatusCode |
                                DataSetFieldContentMask.SourceTimestamp |
                                (legacyCliModel.FullFeaturedMessage ? DataSetFieldContentMask.ServerTimestamp : 0) |
                                DataSetFieldContentMask.NodeId |
                                DataSetFieldContentMask.DisplayName |
                                DataSetFieldContentMask.ApplicationUri |
                                (legacyCliModel.FullFeaturedMessage ? DataSetFieldContentMask.EndpointUrl : 0) |
                                (legacyCliModel.FullFeaturedMessage ? DataSetFieldContentMask.ExtensionFields : 0),
                            MessageSettings = new DataSetWriterMessageSettingsModel() {
                                DataSetMessageContentMask =
                                    (legacyCliModel.FullFeaturedMessage ? DataSetContentMask.Timestamp : 0) |
                                    DataSetContentMask.MetaDataVersion |
                                    DataSetContentMask.DataSetWriterId |
                                    DataSetContentMask.MajorVersion |
                                    DataSetContentMask.MinorVersion |
                                    (legacyCliModel.FullFeaturedMessage ? DataSetContentMask.SequenceNumber : 0)
                            }
                        }))
                    .ToList();
            }
            catch (Exception ex) {
                _logger.Error(ex, "failed to convert published nodes to dataset writers.");
                return Enumerable.Empty<DataSetWriterModel>();
            }
        }

        /// <summary>
        /// Get an id for data source
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public static string Hash(PublishedDataSetSourceModel model) {
            var id = model.Connection?.Endpoint?.Url +
                model.Connection?.Endpoint?.SecurityMode.ToString() +
                model.Connection?.Endpoint?.SecurityPolicy +
                model.Connection?.User?.Type.ToString() +
                model.Connection?.User?.Value.ToJson() +
                model.SubscriptionSettings?.PublishingInterval.ToString() +
                model.PublishedVariables.PublishedData.First()?.Id +
                model.PublishedVariables.PublishedData.First()?.PublishedVariableNodeId +
                model.PublishedVariables.PublishedData.First()?.PublishedVariableDisplayName +
                model.PublishedVariables.PublishedData.First()?.SamplingInterval +
                model.PublishedVariables.PublishedData.First()?.HeartbeatInterval;
            return id.ToSha1Hash();
        }

        /// <summary>
        /// Get the node models from entry
        /// </summary>
        /// <param name="item"></param>
        /// <param name="scaleTestCount"></param>
        /// <returns></returns>
        private IEnumerable<OpcNodeModel> GetNodeModels(PublishedNodesEntryModel item,
            int scaleTestCount = 1) {

            if (item.OpcNodes != null) {
                foreach (var node in item.OpcNodes) {
                    if (string.IsNullOrEmpty(node.Id)) {
                        node.Id = node.ExpandedNodeId;
                    }
                    if (scaleTestCount == 1) {
                        yield return node;
                    }
                    else {
                        for (var i = 0; i < scaleTestCount; i++) {
                            yield return new OpcNodeModel {
                                Id = node.Id,
                                DisplayName = string.IsNullOrEmpty(node.DisplayName) ?
                                    $"{node.Id}_{i}" : $"{node.DisplayName}_{i}",
                                ExpandedNodeId = node.ExpandedNodeId,
                                HeartbeatInterval = node.HeartbeatInterval,
                                HeartbeatIntervalTimespan = node.HeartbeatIntervalTimespan,
                                OpcPublishingInterval = node.OpcPublishingInterval,
                                OpcPublishingIntervalTimespan = node.OpcPublishingIntervalTimespan,
                                OpcSamplingInterval = node.OpcSamplingInterval,
                                OpcSamplingIntervalTimespan = node.OpcSamplingIntervalTimespan,
                                SkipFirst = node.SkipFirst
                            };
                        }
                    }
                }
            }
            if (item.NodeId?.Identifier != null) {
                yield return new OpcNodeModel {
                    Id = item.NodeId.Identifier,
                };
            }
        }

        /// <summary>
        /// Extract publishing interval from nodes
        /// </summary>
        /// <param name="opcNodes"></param>
        /// <param name="legacyCliModel">The legacy command line arguments</param>
        /// <returns></returns>
        private static TimeSpan? GetPublishingIntervalFromNodes(IEnumerable<OpcNodeModel> opcNodes,
            LegacyCliModel legacyCliModel) {
            var interval = opcNodes
                .FirstOrDefault(x => x.OpcPublishingInterval != null)?.OpcPublishingIntervalTimespan;
            return interval ?? legacyCliModel.DefaultPublishingInterval;
        }

        /// <summary>
        /// Convert to credential model
        /// </summary>
        /// <param name="entry"></param>
        /// <returns></returns>
        private async Task<CredentialModel> ToUserNamePasswordCredentialAsync(
            PublishedNodesEntryModel entry) {
            var user = entry.OpcAuthenticationUsername;
            var password = entry.OpcAuthenticationPassword;
            if (string.IsNullOrEmpty(user)) {
                if (_cryptoProvider == null || string.IsNullOrEmpty(entry.EncryptedAuthUsername)) {
                    return null;
                }

                const string kInitializationVector = "alKGJdfsgidfasdO"; // See previous publisher
                var userBytes = await _cryptoProvider.DecryptAsync(kInitializationVector,
                    Convert.FromBase64String(entry.EncryptedAuthUsername));
                user = Encoding.UTF8.GetString(userBytes);
                if (entry.EncryptedAuthPassword != null) {
                    var passwordBytes = await _cryptoProvider.DecryptAsync(kInitializationVector,
                        Convert.FromBase64String(entry.EncryptedAuthPassword));
                    password = Encoding.UTF8.GetString(passwordBytes);
                }
            }
            return new CredentialModel {
                Type = CredentialType.UserName,
                Value = _serializer.FromObject(new { user, password })
            };
        }

        /// <summary>
        /// Convert to user name password
        /// </summary>
        /// <param name="credential"></param>
        /// <param name="user"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        private OpcAuthenticationMode ToUserNamePasswordCredential(CredentialModel credential,
            out string user, out string password) {

            if (credential?.Type != CredentialType.UserName) {
                user = null;
                password = null;
                return OpcAuthenticationMode.Anonymous;
            }

            user = (string)credential.Value[nameof(user)];
            password = (string)credential.Value[nameof(password)];
            return OpcAuthenticationMode.UsernamePassword;
        }

        /// <summary>
        /// Describing an entry in the node list
        /// </summary>
        [DataContract]
        public class OpcNodeModel {

            /// <summary> Node Identifier </summary>
            [DataMember(EmitDefaultValue = false)]
            public string Id { get; set; }

            /// <summary> Also </summary>
            [DataMember(EmitDefaultValue = false)]
            public string ExpandedNodeId { get; set; }

            /// <summary> Sampling interval </summary>
            [DataMember(EmitDefaultValue = false)]
            public int? OpcSamplingInterval { get; set; }

            /// <summary>
            /// OpcSamplingInterval as TimeSpan.
            /// </summary>
            [IgnoreDataMember]
            public TimeSpan? OpcSamplingIntervalTimespan {
                get => OpcSamplingInterval.HasValue ?
                    TimeSpan.FromMilliseconds(OpcSamplingInterval.Value) : (TimeSpan?)null;
                set => OpcSamplingInterval = value != null ?
                    (int)value.Value.TotalMilliseconds : (int?)null;
            }

            /// <summary> Publishing interval </summary>
            [DataMember(EmitDefaultValue = false)]
            public int? OpcPublishingInterval { get; set; }

            /// <summary>
            /// OpcPublishingInterval as TimeSpan.
            /// </summary>
            [IgnoreDataMember]
            public TimeSpan? OpcPublishingIntervalTimespan {
                get => OpcPublishingInterval.HasValue ?
                    TimeSpan.FromMilliseconds(OpcPublishingInterval.Value) : (TimeSpan?)null;
                set => OpcPublishingInterval = value != null ?
                    (int)value.Value.TotalMilliseconds : (int?)null;
            }

            /// <summary> Display name </summary>
            [DataMember(EmitDefaultValue = false)]
            public string DisplayName { get; set; }

            /// <summary> Heartbeat </summary>
            [DataMember(EmitDefaultValue = false)]
            public int? HeartbeatInterval { get; set; }

            /// <summary>
            /// Heartbeat interval as TimeSpan.
            /// </summary>
            [IgnoreDataMember]
            public TimeSpan? HeartbeatIntervalTimespan {
                get => HeartbeatInterval.HasValue ?
                    TimeSpan.FromSeconds(HeartbeatInterval.Value) : (TimeSpan?)null;
                set => HeartbeatInterval = value != null ?
                    (int)value.Value.TotalSeconds : (int?)null;
            }

            /// <summary> Skip first value </summary>
            [DataMember(EmitDefaultValue = false)]
            public bool? SkipFirst { get; set; }
        }

        /// <summary>
        /// Node id serialized as object
        /// </summary>
        [DataContract]
        public class NodeIdModel {
            /// <summary> Identifier </summary>
            [DataMember(EmitDefaultValue = false)]
            public string Identifier { get; set; }
        }

        /// <summary>
        /// Contains the nodes which should be
        /// </summary>
        [DataContract]
        public class PublishedNodesEntryModel {

            /// <summary> The endpoint URL of the OPC UA server. </summary>
            [DataMember(IsRequired = true)]
            public Uri EndpointUrl { get; set; }

            /// <summary> Secure transport should be used to </summary>
            [DataMember(EmitDefaultValue = false)]
            public bool? UseSecurity { get; set; }

            /// <summary> The node to monitor in "ns=" syntax. </summary>
            [DataMember(EmitDefaultValue = false)]
            public NodeIdModel NodeId { get; set; }

            /// <summary> authentication mode </summary>
            [DataMember(EmitDefaultValue = false)]
            public OpcAuthenticationMode OpcAuthenticationMode { get; set; }

            /// <summary> encrypted username </summary>
            [DataMember(EmitDefaultValue = false)]
            public string EncryptedAuthUsername { get; set; }

            /// <summary> encrypted password </summary>
            [DataMember]
            public string EncryptedAuthPassword { get; set; }

            /// <summary> plain username </summary>
            [DataMember(EmitDefaultValue = false)]
            public string OpcAuthenticationUsername { get; set; }

            /// <summary> plain password </summary>
            [DataMember]
            public string OpcAuthenticationPassword { get; set; }

            /// <summary> Nodes defined in the collection. </summary>
            [DataMember(EmitDefaultValue = false)]
            public List<OpcNodeModel> OpcNodes { get; set; }
        }

        /// <summary>
        /// Enum that defines the authentication method
        /// </summary>
        [DataContract]
        public enum OpcAuthenticationMode {
            /// <summary> Anonymous authentication </summary>
            [EnumMember]
            Anonymous,
            /// <summary> Username/Password authentication </summary>
            [EnumMember]
            UsernamePassword
        }

        private readonly LegacyCliModel _legacyCliModel;
        private readonly IJsonSerializer _serializer;
        private readonly ISecureElement _cryptoProvider;
        private readonly ILogger _logger;
    }
}