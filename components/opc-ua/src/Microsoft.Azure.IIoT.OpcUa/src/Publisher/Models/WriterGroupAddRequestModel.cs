// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Models {
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Writer group registration request
    /// </summary>
    public class WriterGroupAddRequestModel {

        /// <summary>
        /// Name of the writer group
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Priority of the writer group
        /// </summary>
        public byte? Priority { get; set; }

        /// <summary>
        /// Site id this writer group applies to (mandatory)
        /// </summary>
        public string SiteId { get; set; }

        /// <summary>
        /// Message encoding to generate (publisher extension)
        /// </summary>
        public MessageEncoding? Encoding { get; set; }

        /// <summary>
        /// The message schema to use (publisher extension)
        /// </summary>
        public MessageSchema? Schema { get; set; }

        /// <summary>
        /// Header layout uri
        /// </summary>
        public string HeaderLayoutUri { get; set; }

        /// <summary>
        /// Batch buffer size
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

        /// <summary>
        /// Network message detailed settings.
        /// </summary>
        public WriterGroupMessageSettingsModel MessageSettings { get; set; }

        /// <summary>
        /// Locales to use
        /// </summary>
        public List<string> LocaleIds { get; set; }
    }
}