// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Models {
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Network message writer group model fully materialized
    /// </summary>
    public class WriterGroupModel {

        /// <summary>
        /// Dataset writer group identifier
        /// </summary>
        public string WriterGroupId { get; set; }

        /// <summary>
        /// Name of the writer group
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Priority of the writer group
        /// </summary>
        public byte? Priority { get; set; }

        /// <summary>
        /// Site id this writer group applies to
        /// </summary>
        public string SiteId { get; set; }

        /// <summary>
        /// Generation id
        /// </summary>
        public string GenerationId { get; set; }

        /// <summary>
        /// Message encoding to generate (publisher extension)
        /// </summary>
        public MessageEncoding? Encoding { get; set; }

        /// <summary>
        /// The message schema to use (publisher extension)
        /// </summary>
        public MessageSchema? Schema { get; set; }

        /// <summary>
        /// The data set writers generating dataset messages in the group
        /// </summary>
        public List<DataSetWriterModel> DataSetWriters { get; set; }

        /// <summary>
        /// Network message configuration
        /// </summary>
        public WriterGroupMessageSettingsModel MessageSettings { get; set; }

        /// <summary>
        /// Locales to use
        /// </summary>
        public List<string> LocaleIds { get; set; }

        /// <summary>
        /// Header layout uri
        /// </summary>
        public string HeaderLayoutUri { get; set; }

        /// <summary>
        /// Security mode
        /// </summary>
        public SecurityMode? SecurityMode { get; set; }

        /// <summary>
        /// Security group to use
        /// </summary>
        public string SecurityGroupId { get; set; }

        /// <summary>
        /// Security key services to use
        /// </summary>
        public List<ConnectionModel> SecurityKeyServices { get; set; }

        /// <summary>
        /// Max network message size
        /// </summary>
        public uint? MaxNetworkMessageSize { get; set; }

        /// <summary>
        /// Batch buffer size (Publisher extension)
        /// </summary>
        public int? BatchSize { get; set; }

        /// <summary>
        /// Publishing interval
        /// </summary>
        public TimeSpan? PublishingInterval { get; set; }

        /// <summary>
        /// Keep alive time
        /// </summary>
        public TimeSpan? KeepAliveTime { get; set; }
    }
}
