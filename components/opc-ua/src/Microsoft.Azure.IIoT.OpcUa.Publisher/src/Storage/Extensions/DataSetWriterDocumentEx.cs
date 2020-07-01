// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Storage.Default {
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;

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
                DataSetMetaDataSendInterval = model.DataSetMetaDataSendInterval,
                KeyFrameCount = model.KeyFrameCount,
                KeyFrameInterval = model.KeyFrameInterval,
                ConfiguredSize = model.MessageSettings?.ConfiguredSize,
                DataSetMessageContentMask = model.MessageSettings?.DataSetMessageContentMask,
                DataSetOffset = model.MessageSettings?.DataSetOffset,
                NetworkMessageNumber = model.MessageSettings?.NetworkMessageNumber,
                CredentialType = model.DataSet?.User?.Type,
                Credential = model.DataSet?.User?.Value,
                EndpointId = model.DataSet?.EndpointId,
                DiagnosticsLevel = model.DataSet?.DiagnosticsLevel,
                OperationTimeout = model.DataSet?.OperationTimeout,
                Updated = model.Updated?.Time,
                UpdatedAuditId = model.Updated?.AuthorityId,
                Created = model.Created?.Time,
                CreatedAuditId = model.Created?.AuthorityId,
                ExtensionFields = model.DataSet?.ExtensionFields,
                DataSetName = model.DataSet?.Name,
                SubscriptionLifeTimeCount = model.DataSet?.SubscriptionSettings?.LifeTimeCount,
                MaxKeepAliveCount = model.DataSet?.SubscriptionSettings?.MaxKeepAliveCount,
                MaxNotificationsPerPublish =
                    model.DataSet?.SubscriptionSettings?.MaxNotificationsPerPublish,
                SubscriptionPriority = model.DataSet?.SubscriptionSettings?.Priority,
                PublishingInterval = model.DataSet?.SubscriptionSettings?.PublishingInterval,
                ResolveDisplayName = model.DataSet?.SubscriptionSettings?.ResolveDisplayName,
                LastResultChange = model.DataSet?.State?.LastResultChange,
                LastResultDiagnostics = model.DataSet?.State?.LastResult?.Diagnostics,
                LastResultErrorMessage = model.DataSet?.State?.LastResult?.ErrorMessage,
                LastResultStatusCode = model.DataSet?.State?.LastResult?.StatusCode,
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
                DataSet = new PublishedDataSetSourceInfoModel {
                    User = document.CredentialType == null ? null :
                        new CredentialModel {
                            Type = document.CredentialType,
                            Value = document.Credential
                        },
                    State = document.LastResultChange == null ? null : new PublishedDataSetSourceStateModel {
                        LastResult = new ServiceResultModel {
                            ErrorMessage = document.LastResultErrorMessage,
                            Diagnostics = document.LastResultDiagnostics,
                            StatusCode = document.LastResultStatusCode,
                        },
                        LastResultChange = document.LastResultChange
                    },
                    OperationTimeout = document.OperationTimeout,
                    DiagnosticsLevel = document.DiagnosticsLevel,
                    EndpointId = document.EndpointId,
                    ExtensionFields = document.ExtensionFields,
                    Name = document.DataSetName,
                    SubscriptionSettings = new PublishedDataSetSourceSettingsModel {
                        LifeTimeCount = document.SubscriptionLifeTimeCount,
                        MaxKeepAliveCount = document.MaxKeepAliveCount,
                        MaxNotificationsPerPublish = document.MaxNotificationsPerPublish,
                        Priority = document.SubscriptionPriority,
                        PublishingInterval = document.PublishingInterval,
                        ResolveDisplayName = document.ResolveDisplayName
                    }
                },
                DataSetFieldContentMask = document.DataSetFieldContentMask,
                DataSetMetaDataSendInterval = document.DataSetMetaDataSendInterval,
                KeyFrameCount = document.KeyFrameCount,
                KeyFrameInterval = document.KeyFrameInterval,
                MessageSettings = new DataSetWriterMessageSettingsModel {
                    ConfiguredSize = document.ConfiguredSize,
                    DataSetMessageContentMask = document.DataSetMessageContentMask,
                    DataSetOffset = document.DataSetOffset,
                    NetworkMessageNumber = document.NetworkMessageNumber
                },
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
    }
}