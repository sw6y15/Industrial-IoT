// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.App.Models {
    using Microsoft.Azure.IIoT.OpcUa.Api.Registry.Models;
    using System;

    public class PublisherInfo {

        /// <summary>
        /// Publisher model
        /// </summary>
        public PublisherApiModel PublisherModel { get; set; }

        public bool TryUpdateData(PublisherInfoRequested input) {
            try {
                if (!string.IsNullOrEmpty(input.RequestedLogLevel)) {
                    PublisherModel.LogLevel = Enum.Parse<TraceLogLevel>(input.RequestedLogLevel);
                }
                else {
                    PublisherModel.LogLevel = null;
                }
                return true;
            }
            catch (Exception) {
                return false;
            }
        }
    }
}
