// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Models {
    using System.Linq;

    /// <summary>
    /// Variables extensions
    /// </summary>
    public static class PublishedDataSetVariableModelEx {

        /// <summary>
        /// Clone
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public static PublishedDataSetVariableModel Clone(this PublishedDataSetVariableModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetVariableModel {
                Id = model.Id,
                GenerationId = model.GenerationId,
                DiscardNew = model.DiscardNew,
                Attribute = model.Attribute,
                DataChangeFilter = model.DataChangeFilter,
                DeadbandType = model.DeadbandType,
                DeadbandValue = model.DeadbandValue,
                IndexRange = model.IndexRange,
                MetaDataProperties = model.MetaDataProperties?.ToList(),
                PublishedVariableNodeId = model.PublishedVariableNodeId,
                PublishedVariableDisplayName = model.PublishedVariableDisplayName,
                SamplingInterval = model.SamplingInterval,
                SubstituteValue = model.SubstituteValue?.Copy(),
                QueueSize = model.QueueSize,
                HeartbeatInterval = model.HeartbeatInterval,
                BrowsePath = model.BrowsePath,
                MonitoringMode = model.MonitoringMode,
                Created = model.Created.Clone(),
                Updated = model.Updated.Clone(),
                TriggerId = model.TriggerId,
                Order = model.Order
            };
        }

        /// <summary>
        /// Get model from request
        /// </summary>
        /// <param name="model"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public static PublishedDataSetVariableModel AsDataSetVariable(
            this DataSetAddVariableRequestModel model,
            PublisherOperationContextModel context) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetVariableModel {
                Id = null,
                GenerationId = null,
                DiscardNew = model.DiscardNew,
                Attribute = model.Attribute,
                DataChangeFilter = model.DataChangeFilter,
                DeadbandType = model.DeadbandType,
                DeadbandValue = model.DeadbandValue,
                IndexRange = model.IndexRange,
                MetaDataProperties = model.MetaDataProperties?.ToList(),
                PublishedVariableNodeId = model.PublishedVariableNodeId,
                PublishedVariableDisplayName = model.PublishedVariableDisplayName,
                SamplingInterval = model.SamplingInterval,
                SubstituteValue = model.SubstituteValue?.Copy(),
                QueueSize = model.QueueSize,
                HeartbeatInterval = model.HeartbeatInterval,
                BrowsePath = model.BrowsePath,
                MonitoringMode = model.MonitoringMode,
                Updated = context,
                Created = context,
                TriggerId = model.TriggerId,
                Order = model.Order
            };
        }
    }
}