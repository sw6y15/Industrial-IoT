// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Models {
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;
    using System;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// Clone
    /// </summary>
    public static class WriterGroupModelEx {

        /// <summary>
        /// Clone
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public static WriterGroupModel Clone(this WriterGroupModel model) {
            if (model?.DataSetWriters == null) {
                return null;
            }
            return new WriterGroupModel {
                WriterGroupId = model.WriterGroupId,
                DataSetWriters = model.DataSetWriters
                    .Select(f => f.Clone())
                    .ToList(),
                HeaderLayoutUri = model.HeaderLayoutUri,
                KeepAliveTime = model.KeepAliveTime,
                GenerationId = model.GenerationId,
                SiteId = model.SiteId,
                LocaleIds = model.LocaleIds?.ToList(),
                MaxNetworkMessageSize = model.MaxNetworkMessageSize,
                MessageSettings = model.MessageSettings.Clone(),
                Encoding = model.Encoding,
                Schema = model.Schema,
                Name = model.Name,
                Priority = model.Priority,
                PublishingInterval = model.PublishingInterval,
                SecurityGroupId = model.SecurityGroupId,
                SecurityKeyServices = model.SecurityKeyServices?
                    .Select(c => c.Clone())
                    .ToList(),
                SecurityMode = model.SecurityMode,
            };
        }

        /// <summary>
        /// Create version hash from generations
        /// </summary>
        /// <returns></returns>
        public static string CalculateVersionHash(this WriterGroupModel model) {
            var sb = new StringBuilder();
            sb.Append(model.GenerationId);
            if (model.DataSetWriters != null) {
                foreach (var writer in model.DataSetWriters) {
                    sb.Append(writer.GenerationId);
                    var dataset =
                        writer.DataSet?.DataSetSource?.PublishedVariables?.PublishedData;
                    if (dataset != null) {
                        foreach (var item in dataset) {
                            sb.Append(item.GenerationId);
                        }
                    }
                    var events = writer.DataSet?.DataSetSource.PublishedEvents;
                    if (events != null) {
                        sb.Append(events.GenerationId);
                    }
                }
            }
            return sb.ToString().ToSha1Hash();
        }
    }
}