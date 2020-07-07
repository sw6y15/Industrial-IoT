// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Models {
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Writer Group model extensions
    /// </summary>
    public static class WriterGroupInfoModelEx {

        /// <summary>
        /// Clone
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public static WriterGroupInfoModel Clone(this WriterGroupInfoModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupInfoModel {
                BatchSize = model.BatchSize,
                PublishingInterval = model.PublishingInterval,
                GenerationId = model.GenerationId,
                HeaderLayoutUri = model.HeaderLayoutUri,
                WriterGroupId = model.WriterGroupId,
                KeepAliveTime = model.KeepAliveTime,
                LocaleIds = model.LocaleIds?.ToList(),
                MaxNetworkMessageSize = model.MaxNetworkMessageSize,
                Encoding = model.Encoding,
                Name = model.Name,
                Priority = model.Priority,
                SiteId = model.SiteId,
                MessageSettings = model.MessageSettings.Clone(),
                Updated = model.Updated.Clone(),
                Created = model.Created.Clone(),
                SecurityGroupId = model.SecurityGroupId,
                SecurityKeyServices = model.SecurityKeyServices?
                    .Select(c => c.Clone())
                    .ToList(),
                SecurityMode = model.SecurityMode
            };
        }

        /// <summary>
        /// Clone
        /// </summary>
        /// <param name="model"></param>
        /// <param name="writers"></param>
        /// <returns></returns>
        public static WriterGroupModel AsWriterGroup(this WriterGroupInfoModel model,
            IEnumerable<DataSetWriterModel> writers) {
            if (model == null) {
                return null;
            }
            return new WriterGroupModel {
                GenerationId = model.GenerationId,
                PublishingInterval = model.PublishingInterval,
                HeaderLayoutUri = model.HeaderLayoutUri,
                WriterGroupId = model.WriterGroupId,
                KeepAliveTime = model.KeepAliveTime,
                LocaleIds = model.LocaleIds?.ToList(),
                MaxNetworkMessageSize = model.MaxNetworkMessageSize,
                Encoding = model.Encoding,
                Schema = model.Schema,
                Name = model.Name,
                BatchSize = model.BatchSize,
                Priority = model.Priority,
                SiteId = model.SiteId,
                DataSetWriters = writers.ToList(),
                MessageSettings = model.MessageSettings.Clone(),
                SecurityGroupId = model.SecurityGroupId,
                SecurityKeyServices = model.SecurityKeyServices?
                    .Select(c => c.Clone())
                    .ToList(),
                SecurityMode = model.SecurityMode
            };
        }

        /// <summary>
        /// Get model from request
        /// </summary>
        /// <param name="model"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public static WriterGroupInfoModel AsWriterGroupInfo(
            this WriterGroupAddRequestModel model,
            PublisherOperationContextModel context) {
            if (model == null) {
                return null;
            }
            return new WriterGroupInfoModel {
                WriterGroupId = null,
                GenerationId = null,
                PublishingInterval = model.PublishingInterval,
                HeaderLayoutUri = model.HeaderLayoutUri,
                KeepAliveTime = model.KeepAliveTime,
                LocaleIds = model.LocaleIds?.ToList(),
                Encoding = model.Encoding,
                Name = model.Name,
                Priority = model.Priority,
                SiteId = model.SiteId,
                MessageSettings = model.MessageSettings.Clone(),
                BatchSize = model.BatchSize,
                Created = context.Clone(),
                Updated = context.Clone(),
                // TODO:
                MaxNetworkMessageSize = null,
                SecurityGroupId = null,
                SecurityKeyServices = null,
                SecurityMode = null
            };
        }

        /// <summary>
        /// Whether it is default group
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public static bool IsDefault(WriterGroupInfoModel model) {
            if (model == null) {
                return false;
            }
            return model.WriterGroupId == model.SiteId;
        }
    }
}