// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Models {
    using System.Runtime.Serialization;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Writer group update request
    /// </summary>
    [DataContract]
    public class WriterGroupUpdateRequestApiModel {

        /// <summary>
        /// Generation id
        /// </summary>
        [DataMember(Name = "generationId", Order = 0)]
        public string GenerationId { get; set; }

        /// <summary>
        /// Name of the writer group
        /// </summary>
        [DataMember(Name = "name", Order = 1,
            EmitDefaultValue = false)]
        public string Name { get; set; }

        /// <summary>
        /// Updated message encoding
        /// </summary>
        [DataMember(Name = "encoding", Order = 2,
            EmitDefaultValue = false)]
        public MessageEncoding? Encoding { get; set; }

        /// <summary>
        /// The message schema to use (publisher extension)
        /// </summary>
        [DataMember(Name = "schema", Order = 3,
            EmitDefaultValue = false)]
        public MessageSchema? Schema { get; set; }

        /// <summary>
        /// Updated network message configuration
        /// </summary>
        [DataMember(Name = "messageSettings", Order = 4,
            EmitDefaultValue = false)]
        public WriterGroupMessageSettingsApiModel MessageSettings { get; set; }

        /// <summary>
        /// Header layout uri
        /// </summary>
        [DataMember(Name = "headerLayoutUri", Order = 5,
            EmitDefaultValue = false)]
        public string HeaderLayoutUri { get; set; }

        /// <summary>
        /// Batch buffer size
        /// </summary>
        [DataMember(Name = "batchSize", Order = 6,
            EmitDefaultValue = false)]
        public int? BatchSize { get; set; }

        /// <summary>
        /// Publishing interval
        /// </summary>
        [DataMember(Name = "publishingInterval", Order = 7,
            EmitDefaultValue = false)]
        public TimeSpan? PublishingInterval { get; set; }

        /// <summary>
        /// Keep alive time
        /// </summary>
        [DataMember(Name = "keepAliveTime", Order = 8,
            EmitDefaultValue = false)]
        public TimeSpan? KeepAliveTime { get; set; }

        /// <summary>
        /// Priority of the writer group
        /// </summary>
        [DataMember(Name = "priority", Order = 9,
            EmitDefaultValue = false)]
        public byte? Priority { get; set; }

        /// <summary>
        /// Locales to use
        /// </summary>
        [DataMember(Name = "localeIds", Order = 10,
            EmitDefaultValue = false)]
        public List<string> LocaleIds { get; set; }
    }
}