// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Models {
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;

    /// <summary>
    /// Dataset source state extensions
    /// </summary>
    public static class PublishedDataSetSourceStateModelEx {

        /// <summary>
        /// Clone
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public static PublishedDataSetSourceStateModel Clone(
            this PublishedDataSetSourceStateModel model) {
            if (model?.LastResultChange == null &&
                model?.LastResult == null &&
                model?.EndpointState == null) {
                return null;
            }
            return new PublishedDataSetSourceStateModel {
                EndpointState = model.EndpointState,
                LastResultChange = model.LastResultChange,
                LastResult = model.LastResult.Clone()
            };
        }
    }
}