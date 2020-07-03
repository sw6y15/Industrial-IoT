// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Storage.Default {
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Opc.Ua;
    using System;

    /// <summary>
    /// Variable model extensions
    /// </summary>
    public static class DataSetEntityDocumentEx {

        /// <summary>
        /// Create unique dataset variable id from dataset writer id
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <param name="variableId"></param>
        /// <returns></returns>
        public static string GetDocumentId(string dataSetWriterId,
            string variableId) {
            return dataSetWriterId + "_" + variableId;
        }

        /// <summary>
        /// Create unique dataset event id from dataset writer id
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <returns></returns>
        public static string GetDocumentId(string dataSetWriterId) {
            return dataSetWriterId + "_EventDefinition";
        }

        /// <summary>
        /// Convert to storage
        /// </summary>
        /// <param name="model"></param>
        /// <param name="dataSetWriterId"></param>
        /// <returns></returns>
        public static DataSetEntityDocument ToDocumentModel(
            this PublishedDataSetEventsModel model, string dataSetWriterId) {
            if (model == null) {
                return null;
            }
            return new DataSetEntityDocument {
                ETag = model.GenerationId,
                DataSetWriterId = dataSetWriterId,
                Id = GetDocumentId(dataSetWriterId),
                BrowsePath = model.BrowsePath,
                DiscardNew = model.DiscardNew,
                MonitoringMode = model.MonitoringMode,
                QueueSize = model.QueueSize,
                TriggerId = model.TriggerId,
                EventNotifier = model.EventNotifier,
                FilterElements = model.Filter?.Elements,
                SelectedFields = model.SelectedFields,
                Order = 0,
                VariableId = null,
                Attribute = null,
                DataChangeFilter = null,
                DeadbandType = null,
                DeadbandValue = null,
                DisplayName = null,
                HeartbeatInterval = null,
                IndexRange = null,
                MetaDataProperties = null,
                NodeId = null,
                SamplingInterval = null,
                SubstituteValue = null,
                Updated = model.Updated?.Time ?? DateTime.UtcNow,
                UpdatedAuditId = model.Updated?.AuthorityId,
                Created = model.Created?.Time ?? DateTime.UtcNow,
                CreatedAuditId = model.Created?.AuthorityId,
                LastResultChange = model.State?.LastResultChange,
                LastResultDiagnostics = model.State?.LastResult?.Diagnostics,
                ServerId = model.State?.ServerId,
                ClientId = model.State?.ClientId,
                LastResultErrorMessage = model.State?.LastResult?.ErrorMessage,
                LastResultStatusCode = model.State?.LastResult?.StatusCode,
                Type = DataSetEntityDocument.EventSet,
                ClassType = DataSetEntityDocument.ClassTypeName
            };
        }

        /// <summary>
        /// Convert to storage
        /// </summary>
        /// <param name="model"></param>
        /// <param name="dataSetWriterId"></param>
        /// <returns></returns>
        public static DataSetEntityDocument ToDocumentModel(
            this PublishedDataSetVariableModel model, string dataSetWriterId) {
            if (model == null) {
                return null;
            }
            return new DataSetEntityDocument {
                ETag = model.GenerationId,
                Id = GetDocumentId(dataSetWriterId, model.Id),
                VariableId = model.Id,
                DataSetWriterId = dataSetWriterId,
                Attribute = model.Attribute,
                BrowsePath = model.BrowsePath,
                DataChangeFilter = model.DataChangeFilter,
                DeadbandType = model.DeadbandType,
                DeadbandValue = model.DeadbandValue,
                DiscardNew = model.DiscardNew,
                DisplayName = model.PublishedVariableDisplayName,
                HeartbeatInterval = model.HeartbeatInterval,
                IndexRange = model.IndexRange,
                MetaDataProperties = model.MetaDataProperties,
                MonitoringMode = model.MonitoringMode,
                NodeId = model.PublishedVariableNodeId,
                QueueSize = model.QueueSize,
                SamplingInterval = model.SamplingInterval,
                SubstituteValue = model.SubstituteValue?.Copy(),
                TriggerId = model.TriggerId,
                EventNotifier = null,
                FilterElements = null,
                SelectedFields = null,
                Order = model.Order ?? 0,
                LastResultChange = model.State?.LastResultChange,
                LastResultDiagnostics = model.State?.LastResult?.Diagnostics,
                ServerId = model.State?.ServerId,
                ClientId = model.State?.ClientId,
                LastResultErrorMessage = model.State?.LastResult?.ErrorMessage,
                LastResultStatusCode = model.State?.LastResult?.StatusCode,
                Updated = model.Updated?.Time ?? DateTime.UtcNow,
                UpdatedAuditId = model.Updated?.AuthorityId,
                Created = model.Created?.Time ?? DateTime.UtcNow,
                CreatedAuditId = model.Created?.AuthorityId,
                Type = DataSetEntityDocument.Variable,
                ClassType = DataSetEntityDocument.ClassTypeName
            };
        }

        /// <summary>
        /// Convert to Service model
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        public static PublishedDataSetEventsModel ToEventDataSetModel(
            this DataSetEntityDocument document) {
            if (document == null) {
                return null;
            }
            return new PublishedDataSetEventsModel {
                GenerationId = document.ETag,
                Id = document.DataSetWriterId,
                BrowsePath = document.BrowsePath,
                DiscardNew = document.DiscardNew,
                MonitoringMode = document.MonitoringMode,
                QueueSize = document.QueueSize,
                TriggerId = document.TriggerId,
                EventNotifier = document.EventNotifier,
                Filter = new ContentFilterModel {
                    Elements = document.FilterElements,
                },
                SelectedFields = document.SelectedFields,
                State = ToDataSetItemState(document),
                Updated = document.Updated == null ? null : new PublisherOperationContextModel {
                    Time = document.Updated.Value,
                    AuthorityId = document.UpdatedAuditId
                },
                Created = document.Created == null ? null : new PublisherOperationContextModel {
                    Time = document.Created.Value,
                    AuthorityId = document.CreatedAuditId
                }
            };
        }

        /// <summary>
        /// Convert to Service model
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        public static PublishedDataSetVariableModel ToDataSetVariableModel(
            this DataSetEntityDocument document) {
            if (document == null) {
                return null;
            }
            return new PublishedDataSetVariableModel {
                Id = document.VariableId,
                GenerationId = document.ETag,
                Attribute = document.Attribute,
                BrowsePath = document.BrowsePath,
                DataChangeFilter = document.DataChangeFilter,
                DeadbandType = document.DeadbandType,
                DeadbandValue = document.DeadbandValue,
                DiscardNew = document.DiscardNew,
                PublishedVariableDisplayName = document.DisplayName,
                HeartbeatInterval = document.HeartbeatInterval,
                IndexRange = document.IndexRange,
                MetaDataProperties = document.MetaDataProperties,
                MonitoringMode = document.MonitoringMode,
                PublishedVariableNodeId = document.NodeId,
                QueueSize = document.QueueSize,
                Order = document.Order,
                SamplingInterval = document.SamplingInterval,
                SubstituteValue = document.SubstituteValue?.Copy(),
                TriggerId = document.TriggerId,
                State = ToDataSetItemState(document),
                Updated = document.Updated == null ? null : new PublisherOperationContextModel {
                    Time = document.Updated.Value,
                    AuthorityId = document.UpdatedAuditId
                },
                Created = document.Created == null ? null : new PublisherOperationContextModel {
                    Time = document.Created.Value,
                    AuthorityId = document.CreatedAuditId
                }
            };
        }

        /// <summary>
        /// Create state
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        private static PublishedDataSetItemStateModel ToDataSetItemState(DataSetEntityDocument document) {
            var lastResult = ToServiceResultModel(document);
            if (lastResult == null &&
                document.LastResultChange == null &&
                (document.ServerId ?? 0u) == 0u &&
                (document.ClientId ?? 0u) == 0u) {
                return null;
            }
            return new PublishedDataSetItemStateModel {
                LastResult = lastResult,
                ServerId = document.ServerId == 0u ? null : document.ServerId,
                ClientId = document.ClientId == 0u ? null : document.ClientId,
                LastResultChange = document.LastResultChange
            };
        }

        /// <summary>
        /// Create service result model
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        private static ServiceResultModel ToServiceResultModel(DataSetEntityDocument document) {
            if (document.LastResultDiagnostics == null &&
                document.LastResultStatusCode == null &&
                string.IsNullOrEmpty(document.LastResultErrorMessage)) {
                return null;
            }
            return new ServiceResultModel {
                ErrorMessage = document.LastResultErrorMessage,
                Diagnostics = document.LastResultDiagnostics,
                StatusCode = document.LastResultStatusCode,
            };
        }
    }
}