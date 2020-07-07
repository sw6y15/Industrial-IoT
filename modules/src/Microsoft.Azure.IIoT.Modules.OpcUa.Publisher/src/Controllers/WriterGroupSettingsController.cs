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
        /// Network message schema to produce
        /// </summary>
        public string MessageSchema {
            get => _engine.MessageSchema;
            set => _engine.MessageSchema = value;
        }

        /// <summary>
        /// Dataset writer group identifier
        /// </summary>
        public string WriterGroupId {
            get => _engine.WriterGroupId;
            set => _engine.WriterGroupId = value;
        }

        /// <summary>
        /// Group version
        /// </summary>
        public uint? GroupVersion {
            get => _engine.GroupVersion;
            set => _engine.GroupVersion = value;
        }

        /// <summary>
        /// Max network message size
        /// </summary>
        public uint? MaxNetworkMessageSize {
            get => _engine.MaxNetworkMessageSize;
            set => _engine.MaxNetworkMessageSize = value;
        }

        /// <summary>
        /// Header layout uri
        /// </summary>
        public string HeaderLayoutUri {
            get => _engine.HeaderLayoutUri;
            set => _engine.HeaderLayoutUri = value;
        }

        /// <summary>
        /// Batch buffer size (Publisher extension)
        /// </summary>
        public int? BatchSize {
            get => _engine.BatchSize;
            set => _engine.BatchSize = value;
        }

        /// <summary>
        /// Publishing interval
        /// </summary>
        public TimeSpan? PublishingInterval {
            get => _engine.PublishingInterval;
            set => _engine.PublishingInterval = value;
        }

        /// <summary>
        /// Keep alive time
        /// </summary>
        public TimeSpan? KeepAliveTime {
            get => _engine.KeepAliveTime;
            set => _engine.KeepAliveTime = value;
        }

        /// <summary>
        /// Uadp Sampling offset
        /// </summary>
        public double? SamplingOffset {
            get => _engine.SamplingOffset;
            set => _engine.SamplingOffset = value;
        }

        /// <summary>
        /// Publishing offset for uadp messages
        /// </summary>
        public Dictionary<string, double> PublishingOffset {
            get => _engine.PublishingOffset.EncodeAsDictionary();
            set => _engine.PublishingOffset = value.DecodeAsList();
        }

        /// <summary>
        /// Priority of the writer group
        /// </summary>
        public byte? Priority {
            get => _engine.Priority;
            set => _engine.Priority = value;
        }

        /// <summary>
        /// Uadp dataset ordering
        /// </summary>
        public DataSetOrderingType? DataSetOrdering {
            get => (DataSetOrderingType?)_engine.DataSetOrdering;
            set => _engine.DataSetOrdering =
                (IIoT.OpcUa.Publisher.Models.DataSetOrderingType?)value;
        }

        /// <summary>
        /// Network message content
        /// </summary>
        public NetworkMessageContentMask? NetworkMessageContentMask {
            get => (NetworkMessageContentMask?)_engine.NetworkMessageContentMask;
            set => _engine.NetworkMessageContentMask =
                (IIoT.OpcUa.Publisher.Models.NetworkMessageContentMask?)value;
        }

        //  /// <summary>
        //  /// State of the processor
        //  /// </summary>
        //  public ProcessorState State {
        //      get => _processor.State;
        //      set { /* Only reporting */ }
        //  }

        /// <summary>
        /// Create controller with service
        /// </summary>
        public WriterGroupSettingsController(IWriterGroupProcessingEngine engine) {
            _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        }

        private readonly IWriterGroupProcessingEngine _engine;
    }
}
