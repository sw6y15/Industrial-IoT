// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Models {
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;
    using Microsoft.Azure.IIoT.OpcUa.Registry.Models;
    using System.Linq;

    /// <summary>
    /// Published dataset extensions
    /// </summary>
    public static class PublishedDataSetModelEx {

        /// <summary>
        /// Clone
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public static PublishedDataSetModel Clone(this PublishedDataSetModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetModel {
                DataSetMetaData = model.DataSetMetaData.Clone(),
                DataSetSource = model.DataSetSource.Clone(),
                ExtensionFields = model.ExtensionFields?
                    .ToDictionary(k => k.Key, v => v.Value),
                Name = model.Name
            };
        }

        /// <summary>
        /// Convert to info model
        /// </summary>
        /// <param name="model"></param>
        /// <param name="endpointId"></param>
        /// <returns></returns>
        public static PublishedDataSetSourceInfoModel AsPublishedDataSetSourceInfo(
            this PublishedDataSetModel model, string endpointId) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetSourceInfoModel {
                User = model.DataSetSource?.Connection?.User.Clone(),
                OperationTimeout = model.DataSetSource?.Connection?.OperationTimeout,
                DiagnosticsLevel = model.DataSetSource?.Connection?.Diagnostics?.Level,
                SubscriptionSettings = model.DataSetSource?.SubscriptionSettings.Clone(),
                EndpointId = endpointId,
                ExtensionFields = model.ExtensionFields,
                Name = model.Name,
                State = null
            };
        }
    }
}