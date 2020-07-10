// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Modules.OpcUa.Publisher.Controllers {
    using Microsoft.Azure.IIoT.Module.Framework;
    using Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher;
    using Microsoft.Azure.IIoT.Hub;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Writer group settings controller
    /// </summary>
    [Version(1)]
    [Version(2)]
    public class WriterGroupSettingsController : ISettingsController {

        /// <summary>
        /// Dataset writer group identifier
        /// </summary>
        public string WriterGroupId {
            get => _emitter.WriterGroupId;
            set => _emitter.WriterGroupId = value;
        }

        /// <summary>
        /// Max network message size
        /// </summary>
        public uint? MaxNetworkMessageSize {
            get => _emitter.MaxNetworkMessageSize;
            set => _emitter.MaxNetworkMessageSize = value;
        }

        /// <summary>
        /// Message schema to produce
        /// </summary>
        public MessageSchema? Schema {
            get => (MessageSchema?)_emitter.Schema;
            set => _emitter.Schema =
                (IIoT.OpcUa.Publisher.Models.MessageSchema?)value;
        }

        /// <summary>
        /// Message encoding to use
        /// </summary>
        public MessageEncoding? Encoding {
            get => (MessageEncoding?)_emitter.Encoding;
            set => _emitter.Encoding =
                (IIoT.OpcUa.Publisher.Models.MessageEncoding?)value;
        }

        /// <summary>
        /// Header layout uri
        /// </summary>
        public string HeaderLayoutUri {
            get => _emitter.HeaderLayoutUri;
            set => _emitter.HeaderLayoutUri = value;
        }

        /// <summary>
        /// Batch buffer size (Publisher extension)
        /// </summary>
        public int? BatchSize {
            get => _emitter.BatchSize;
            set => _emitter.BatchSize = value;
        }

        /// <summary>
        /// Publishing interval
        /// </summary>
        public TimeSpan? PublishingInterval {
            get => _emitter.PublishingInterval;
            set => _emitter.PublishingInterval = value;
        }

        /// <summary>
        /// Publishing offset for uadp messages
        /// </summary>
        public Dictionary<string, double> PublishingOffset {
            get => _emitter.PublishingOffset.EncodeAsDictionary();
            set => _emitter.PublishingOffset = value.DecodeAsList();
        }

        /// <summary>
        /// Uadp dataset ordering
        /// </summary>
        public DataSetOrderingType? DataSetOrdering {
            get => (DataSetOrderingType?)_emitter.DataSetOrdering;
            set => _emitter.DataSetOrdering =
                (IIoT.OpcUa.Publisher.Models.DataSetOrderingType?)value;
        }

        /// <summary>
        /// Network message content
        /// </summary>
        public NetworkMessageContentMask? NetworkMessageContentMask {
            get => (NetworkMessageContentMask?)_emitter.MessageContentMask;
            set => _emitter.MessageContentMask =
                (IIoT.OpcUa.Publisher.Models.NetworkMessageContentMask?)value;
        }

        /// <summary>
        /// Group version
        /// </summary>
        public uint? GroupVersion {
            get => _collector.GroupVersion;
            set => _collector.GroupVersion = value;
        }

        /// <summary>
        /// Keep alive time
        /// </summary>
        public TimeSpan? KeepAliveTime {
            get => _collector.KeepAliveTime;
            set => _collector.KeepAliveTime = value;
        }

        /// <summary>
        /// Uadp Sampling offset
        /// </summary>
        public double? SamplingOffset {
            get => _collector.SamplingOffset;
            set => _collector.SamplingOffset = value;
        }

        /// <summary>
        /// Priority of the writer group
        /// </summary>
        public byte? Priority {
            get => _collector.Priority;
            set => _collector.Priority = value;
        }

        //  /// <summary>
        //  /// State of the processor
        //  /// </summary>
        //  public ProcessorState State {
        //      get => _collector.State;
        //      set { /* Only reporting */ }
        //  }

        /// <summary>
        /// Create controller with service
        /// </summary>
        /// <param name="collector"></param>
        /// <param name="emitter"></param>
        public WriterGroupSettingsController(IWriterGroupDataCollector collector,
            IWriterGroupMessageEmitter emitter) {
            _collector = collector ?? throw new ArgumentNullException(nameof(collector));
            _emitter = emitter ?? throw new ArgumentNullException(nameof(emitter));
        }

        private readonly IWriterGroupDataCollector _collector;
        private readonly IWriterGroupMessageEmitter _emitter;
    }
}
