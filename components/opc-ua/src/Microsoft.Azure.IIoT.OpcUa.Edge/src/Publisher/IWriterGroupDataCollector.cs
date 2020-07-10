// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Manages a group of writers all producing dataset messages
    /// </summary>
    public interface IWriterGroupDataCollector {

        /// <summary>
        /// Group version
        /// </summary>
        uint? GroupVersion { get; set; }

        /// <summary>
        /// Keep alive time
        /// </summary>
        TimeSpan? KeepAliveTime { get; set; }

        /// <summary>
        /// Uadp Sampling offset
        /// </summary>
        double? SamplingOffset { get; set; }

        /// <summary>
        /// Priority of the writer group in relation to other groups
        /// </summary>
        byte? Priority { get; set; }

        /// <summary>
        /// Add writers to the group
        /// </summary>
        /// <param name="dataSetWriters"></param>
        /// <returns></returns>
        void AddWriters(IEnumerable<DataSetWriterModel> dataSetWriters);

        /// <summary>
        /// Remove writers from the group
        /// </summary>
        /// <param name="dataSetWriters"></param>
        /// <returns></returns>
        void RemoveWriters(IEnumerable<string> dataSetWriters);

        /// <summary>
        /// Remove all writers
        /// </summary>
        void RemoveAllWriters();
    }
}