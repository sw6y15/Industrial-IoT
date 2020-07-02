// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Storage.Default {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using System;
    using System.Linq;

    /// <summary>
    /// Writer Group model extensions
    /// </summary>
    public static class WriterGroupDocumentEx {

        /// <summary>
        /// Convert to storage
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public static WriterGroupDocument ToDocumentModel(this WriterGroupInfoModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupDocument {
                BatchSize = model.BatchSize,
                PublishingInterval = model.PublishingInterval,
                DataSetOrdering = model.MessageSettings?.DataSetOrdering,
                ETag = model.GenerationId,
                GroupVersion = model.MessageSettings?.GroupVersion,
                HeaderLayoutUri = model.HeaderLayoutUri,
                Id = model.WriterGroupId,
                KeepAliveTime = model.KeepAliveTime,
                LocaleIds = model.LocaleIds?.ToList(),
                MaxNetworkMessageSize = model.MaxNetworkMessageSize,
                MessageType = model.MessageType,
                Name = model.Name,
                NetworkMessageContentMask = model.MessageSettings?.NetworkMessageContentMask,
                Priority = model.Priority,
                PublishingOffset = model.MessageSettings?.PublishingOffset,
                SamplingOffset = model.MessageSettings?.SamplingOffset,
                SiteId = model.SiteId,
                Updated = model.Updated?.Time ?? DateTime.UtcNow,
                UpdatedAuditId = model.Updated?.AuthorityId,
                Created = model.Created?.Time ?? DateTime.UtcNow,
                CreatedAuditId = model.Created?.AuthorityId,
                LastState = model.State?.State ?? WriterGroupState.Disabled,
                LastStateChange = model.State?.LastStateChange ?? DateTime.UtcNow,
                ClassType = WriterGroupDocument.ClassTypeName
            };
        }

        /// <summary>
        /// Convert to Service model
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        public static WriterGroupInfoModel ToFrameworkModel(this WriterGroupDocument document) {
            if (document == null) {
                return null;
            }
            return new WriterGroupInfoModel {
                BatchSize = document.BatchSize,
                PublishingInterval = document.PublishingInterval,
                GenerationId = document.ETag,
                HeaderLayoutUri = document.HeaderLayoutUri,
                WriterGroupId = document.Id,
                KeepAliveTime = document.KeepAliveTime,
                LocaleIds = document.LocaleIds?.ToList(),
                MaxNetworkMessageSize = document.MaxNetworkMessageSize,
                MessageType = document.MessageType,
                Name = document.Name,
                Priority = document.Priority,
                SiteId = document.SiteId,
                MessageSettings = new WriterGroupMessageSettingsModel {
                    DataSetOrdering = document.DataSetOrdering,
                    GroupVersion = document.GroupVersion,
                    NetworkMessageContentMask = document.NetworkMessageContentMask,
                    PublishingOffset = document.PublishingOffset,
                    SamplingOffset = document.SamplingOffset
                },
                State = new WriterGroupStateModel {
                    State = document.LastState,
                    LastStateChange = document.LastStateChange
                },
                Updated = document.Updated == null ? null : new PublisherOperationContextModel {
                    Time = document.Updated.Value,
                    AuthorityId = document.UpdatedAuditId
                },
                Created = document.Created == null ? null : new PublisherOperationContextModel {
                    Time = document.Created.Value,
                    AuthorityId = document.CreatedAuditId
                },
                SecurityGroupId = null,
                SecurityKeyServices = null,
                SecurityMode = null // TODO
            };
        }
    }
}