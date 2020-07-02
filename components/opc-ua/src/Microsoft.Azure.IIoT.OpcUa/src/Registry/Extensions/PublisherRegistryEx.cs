// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Registry {
    using Microsoft.Azure.IIoT.Exceptions;
    using Microsoft.Azure.IIoT.OpcUa.Registry.Models;
    using Microsoft.Azure.IIoT.Hub;
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Publisher registry extensions
    /// </summary>
    public static class PublisherRegistryEx {

        /// <summary>
        /// Convert a writer Group Id to device id
        /// </summary>
        /// <param name="writerGroupId"></param>
        /// <returns></returns>
        public static string ToDeviceId(string writerGroupId) {
            return kWriterGroupDeviceIdPrefix + writerGroupId;
        }

        /// <summary>
        /// Returns writer group id from device id
        /// </summary>
        /// <param name="deviceId"></param>
        /// <returns></returns>
        public static string ToWriterGroupId(string deviceId) {
            if (string.IsNullOrEmpty(deviceId)) {
                return null;
            }
            if (deviceId.StartsWith(kWriterGroupDeviceIdPrefix)) {
                return deviceId.Substring(kWriterGroupDeviceIdPrefix.Length);
            }
            return null;
        }

        /// <summary>
        /// Convert a writer id to a property name
        /// </summary>
        /// <param name="dataSetWriterId"></param>
        /// <returns></returns>
        public static string ToPropertyName(string dataSetWriterId) {
            return IdentityType.DataSet + "_" + dataSetWriterId;
        }

        /// <summary>
        /// Returns writer id from property name
        /// </summary>
        /// <param name="propertyName"></param>
        /// <returns></returns>
        public static string ToDataSetWriterId(string propertyName) {
            if (string.IsNullOrEmpty(propertyName)) {
                return null;
            }
            if (propertyName.StartsWith(IdentityType.DataSet)) {
                return propertyName.Replace(IdentityType.DataSet + "_", "");
            }
            throw new ArgumentException("Not a data set writer id");
        }

        /// <summary>
        /// Find publisher.
        /// </summary>
        /// <param name="service"></param>
        /// <param name="publisherId"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public static async Task<PublisherModel> FindPublisherAsync(
            this IPublisherRegistry service, string publisherId,
            CancellationToken ct = default) {
            try {
                return await service.GetPublisherAsync(publisherId, false, ct);
            }
            catch (ResourceNotFoundException) {
                return null;
            }
        }

        /// <summary>
        /// List all publishers
        /// </summary>
        /// <param name="service"></param>
        /// <param name="onlyServerState"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public static async Task<List<PublisherModel>> ListAllPublishersAsync(
            this IPublisherRegistry service, bool onlyServerState = false,
            CancellationToken ct = default) {
            var publishers = new List<PublisherModel>();
            var result = await service.ListPublishersAsync(null, onlyServerState, null, ct);
            publishers.AddRange(result.Items);
            while (result.ContinuationToken != null) {
                result = await service.ListPublishersAsync(result.ContinuationToken,
                    onlyServerState, null, ct);
                publishers.AddRange(result.Items);
            }
            return publishers;
        }

        /// <summary>
        /// Query all publishers
        /// </summary>
        /// <param name="service"></param>
        /// <param name="query"></param>
        /// <param name="onlyServerState"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public static async Task<List<PublisherModel>> QueryAllPublishersAsync(
            this IPublisherRegistry service, PublisherQueryModel query,
            bool onlyServerState = false, CancellationToken ct = default) {
            var supervisors = new List<PublisherModel>();
            var result = await service.QueryPublishersAsync(query, onlyServerState, null, ct);
            supervisors.AddRange(result.Items);
            while (result.ContinuationToken != null) {
                result = await service.ListPublishersAsync(result.ContinuationToken,
                    onlyServerState, null, ct);
                supervisors.AddRange(result.Items);
            }
            return supervisors;
        }

        private const string kWriterGroupDeviceIdPrefix = "job_";
    }
}
