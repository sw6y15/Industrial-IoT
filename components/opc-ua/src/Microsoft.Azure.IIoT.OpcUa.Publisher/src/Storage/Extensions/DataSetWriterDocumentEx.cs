// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Storage.Default {
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.Serializers;
    using System;

    /// <summary>
    /// DataSet Writer model extensions
    /// </summary>
    public static class DataSetWriterDocumentEx {

        /// <summary>
        /// Convert to storage
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public static DataSetWriterDocument ToDocumentModel(this DataSetWriterInfoModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterDocument {
                Id = model.DataSetWriterId,
                ETag = model.GenerationId,
                WriterGroupId = model.WriterGroupId,
                DataSetFieldContentMask = model.DataSetFieldContentMask,
                IsDisabled = model.IsDisabled ?? false,
                DataSetMetaDataSendInterval = model.DataSetMetaDataSendInterval == TimeSpan.Zero ?
                    null : model.DataSetMetaDataSendInterval,
                KeyFrameCount = model.KeyFrameCount == 0 ?
                    null : model.KeyFrameCount,
                KeyFrameInterval = model.KeyFrameInterval == TimeSpan.Zero ?
                    null : model.KeyFrameInterval,
                ConfiguredSize = model.MessageSettings?.ConfiguredSize == 0 ?
                    null : model.MessageSettings?.ConfiguredSize,
                DataSetMessageContentMask = model.MessageSettings?.DataSetMessageContentMask,
                DataSetOffset = model.MessageSettings?.DataSetOffset == 0 ?
                    null : model.MessageSettings?.DataSetOffset,
                NetworkMessageNumber = model.MessageSettings?.NetworkMessageNumber == 0 ?
                    null : model.MessageSettings?.NetworkMessageNumber,
                CredentialType = model.DataSet?.User?.Type,
                Credential = model.DataSet?.User?.Value,
                EndpointId = model.DataSet?.EndpointId,
                DiagnosticsLevel = model.DataSet?.DiagnosticsLevel,
                OperationTimeout = model.DataSet?.OperationTimeout == TimeSpan.Zero ?
                    null : model.DataSet?.OperationTimeout,
                Updated = model.Updated?.Time,
                UpdatedAuditId = model.Updated?.AuthorityId,
                Created = model.Created?.Time,
                CreatedAuditId = model.Created?.AuthorityId,
                ExtensionFields = model.DataSet?.ExtensionFields?.Count == 0 ? null :
                    model.DataSet?.ExtensionFields,
                DataSetName = model.DataSet?.Name,
                SubscriptionLifeTimeCount = model.DataSet?.SubscriptionSettings?.LifeTimeCount == 0 ?
                    null : model.DataSet?.SubscriptionSettings?.LifeTimeCount,
                MaxKeepAliveCount = model.DataSet?.SubscriptionSettings?.MaxKeepAliveCount == 0 ?
                    null : model.DataSet?.SubscriptionSettings?.MaxKeepAliveCount,
                MaxNotificationsPerPublish =
                        model.DataSet?.SubscriptionSettings?.MaxNotificationsPerPublish == 0 ?
                    null : model.DataSet?.SubscriptionSettings?.MaxNotificationsPerPublish,
                SubscriptionPriority = model.DataSet?.SubscriptionSettings?.Priority == 0 ?
                    null : model.DataSet?.SubscriptionSettings?.Priority,
                PublishingInterval =
                        model.DataSet?.SubscriptionSettings?.PublishingInterval == TimeSpan.Zero ?
                    null : model.DataSet?.SubscriptionSettings?.PublishingInterval,
                ResolveDisplayName = model.DataSet?.SubscriptionSettings?.ResolveDisplayName == false ?
                    null : model.DataSet?.SubscriptionSettings?.ResolveDisplayName,
                LastResultChange = model.DataSet?.State?.LastResultChange,
                EndpointState = model.DataSet?.State?.EndpointState == EndpointConnectivityState.Disconnected ?
                    null : model.DataSet?.State?.EndpointState,
                LastResultDiagnostics = model.DataSet?.State?.LastResult?.Diagnostics,
                LastResultErrorMessage = model.DataSet?.State?.LastResult?.ErrorMessage,
                LastResultStatusCode = model.DataSet?.State?.LastResult?.StatusCode == 0 ?
                    null : model.DataSet?.State?.LastResult?.StatusCode,
                ClassType = DataSetWriterDocument.ClassTypeName
            };
        }

        /// <summary>
        /// Convert to Service model
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        public static DataSetWriterInfoModel ToFrameworkModel(this DataSetWriterDocument document) {
            if (document == null) {
                return null;
            }
            return new DataSetWriterInfoModel {
                DataSetWriterId = document.Id,
                GenerationId = document.ETag,
                WriterGroupId = document.WriterGroupId,
                IsDisabled = document.IsDisabled ? true : (bool?)null,
                DataSet = ToDataSetSourceInfo(document),
                DataSetFieldContentMask = document.DataSetFieldContentMask,
                DataSetMetaDataSendInterval = document.DataSetMetaDataSendInterval,
                KeyFrameCount = document.KeyFrameCount,
                KeyFrameInterval = document.KeyFrameInterval,
                MessageSettings = ToMessageSettings(document),
                Created = document.Created == null ? null : new PublisherOperationContextModel {
                    AuthorityId = document.CreatedAuditId,
                    Time = document.Created.Value
                },
                Updated = document.Updated == null ? null : new PublisherOperationContextModel {
                    AuthorityId = document.UpdatedAuditId,
                    Time = document.Updated.Value
                }
            };
        }

        /// <summary>
        /// Convert to message settings
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        private static DataSetWriterMessageSettingsModel ToMessageSettings(DataSetWriterDocument document) {
            return new DataSetWriterMessageSettingsModel {
                ConfiguredSize = document.ConfiguredSize,
                DataSetMessageContentMask = document.DataSetMessageContentMask,
                DataSetOffset = document.DataSetOffset,
                NetworkMessageNumber = document.NetworkMessageNumber
            };
        }

        /// <summary>
        /// To dataset source info
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        private static PublishedDataSetSourceInfoModel ToDataSetSourceInfo(DataSetWriterDocument document) {
            var subscriptionSettings = ToSubscriptionSettings(document);
            if (subscriptionSettings == null &&
                document.CredentialType == null &&
                document.OperationTimeout == null &&
                document.DiagnosticsLevel == null &&
                document.EndpointId == null &&
                document.ExtensionFields == null &&
                string.IsNullOrEmpty(document.DataSetName)) {
                return null;
            }
            return new PublishedDataSetSourceInfoModel {
                User = document.CredentialType == null ? null : new CredentialModel {
                    Type = document.CredentialType,
                    Value = document.Credential
                },
                State = ToDataSetSourceState(document),
                OperationTimeout = document.OperationTimeout,
                DiagnosticsLevel = document.DiagnosticsLevel,
                EndpointId = document.EndpointId,
                ExtensionFields = document.ExtensionFields,
                Name = document.DataSetName,
                SubscriptionSettings = subscriptionSettings
            };
        }

        /// <summary>
        /// Create subscription settings
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        private static PublishedDataSetSourceSettingsModel ToSubscriptionSettings(DataSetWriterDocument document) {
            return new PublishedDataSetSourceSettingsModel {
                LifeTimeCount = document.SubscriptionLifeTimeCount,
                MaxKeepAliveCount = document.MaxKeepAliveCount,
                MaxNotificationsPerPublish = document.MaxNotificationsPerPublish,
                Priority = document.SubscriptionPriority,
                PublishingInterval = document.PublishingInterval,
                ResolveDisplayName = document.ResolveDisplayName
            };
        }

        /// <summary>
        /// Create state
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        private static PublishedDataSetSourceStateModel ToDataSetSourceState(DataSetWriterDocument document) {
            var lastResult = ToServiceResultModel(document);
            if (lastResult == null &&
                document?.LastResultChange == null &&
                document?.EndpointState == null) {
                return null;
            }
            return new PublishedDataSetSourceStateModel {
                LastResult = lastResult,
                LastResultChange = document.LastResultChange,
                EndpointState = document.EndpointState
            };
        }

        /// <summary>
        /// Create service result model
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        private static ServiceResultModel ToServiceResultModel(DataSetWriterDocument document) {
            if (document.LastResultDiagnostics.IsNull() &&
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