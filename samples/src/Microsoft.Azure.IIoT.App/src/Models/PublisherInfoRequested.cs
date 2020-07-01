// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.App.Models {
    using Microsoft.Azure.IIoT.OpcUa.Api.Registry.Models;
    using System;

    public class PublisherInfoRequested {

        public PublisherInfoRequested(PublisherInfo publisher) {
            _logLevel = publisher?.PublisherModel?.LogLevel;
        }

        private TraceLogLevel? _logLevel;

        /// <summary>
        /// MaxWorkers
        /// </summary>
        public string RequestedLogLevel {
            get => _logLevel != null ?
                _logLevel.Value.ToString() : null;
            set => _logLevel = string.IsNullOrWhiteSpace(value) ?
                (TraceLogLevel?)null : Enum.Parse<TraceLogLevel>(value);
        }
    }
}
