// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Dataset Writer registry extensions
    /// </summary>
    public static class DataSetWriterRegistryEx {

        /// <summary>
        /// Find dataset writers using query
        /// </summary>
        /// <param name="service"></param>
        /// <param name="query"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public static async Task<List<DataSetWriterInfoModel>> QueryAllDataSetWritersAsync(
            this IDataSetWriterRegistry service, DataSetWriterInfoQueryModel query,
            CancellationToken ct = default) {
            var registrations = new List<DataSetWriterInfoModel>();
            var result = await service.QueryDataSetWritersAsync(query, null, ct);
            registrations.AddRange(result.DataSetWriters);
            while (result.ContinuationToken != null) {
                result = await service.ListDataSetWritersAsync(result.ContinuationToken,
                    null, ct);
                registrations.AddRange(result.DataSetWriters);
            }
            return registrations;
        }

        /// <summary>
        /// List all dataset writers
        /// </summary>
        /// <param name="service"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public static async Task<List<DataSetWriterInfoModel>> ListAllDataSetWritersAsync(
            this IDataSetWriterRegistry service, CancellationToken ct = default) {
            var registrations = new List<DataSetWriterInfoModel>();
            var result = await service.ListDataSetWritersAsync(null, null, ct);
            registrations.AddRange(result.DataSetWriters);
            while (result.ContinuationToken != null) {
                result = await service.ListDataSetWritersAsync(result.ContinuationToken,
                    null, ct);
                registrations.AddRange(result.DataSetWriters);
            }
            return registrations;
        }

        /// <summary>
        /// Find dataset variables using query
        /// </summary>
        /// <param name="service"></param>
        /// <param name="query"></param>
        /// <param name="dataSetWriterId"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public static async Task<List<PublishedDataSetVariableModel>> QueryAllDataSetVariablesAsync(
            this IDataSetWriterRegistry service, string dataSetWriterId,
            PublishedDataSetVariableQueryModel query, CancellationToken ct = default) {
            var registrations = new List<PublishedDataSetVariableModel>();
            var result = await service.QueryDataSetVariablesAsync(dataSetWriterId, query, null, ct);
            registrations.AddRange(result.Variables);
            while (result.ContinuationToken != null) {
                result = await service.ListDataSetVariablesAsync(dataSetWriterId, result.ContinuationToken,
                    null, ct);
                registrations.AddRange(result.Variables);
            }
            return registrations;
        }

        /// <summary>
        /// List all dataset variables
        /// </summary>
        /// <param name="service"></param>
        /// <param name="dataSetWriterId"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public static async Task<List<PublishedDataSetVariableModel>> ListAllDataSetVariablesAsync(
            this IDataSetWriterRegistry service, string dataSetWriterId, CancellationToken ct = default) {
            var registrations = new List<PublishedDataSetVariableModel>();
            var result = await service.ListDataSetVariablesAsync(dataSetWriterId, null, null, ct);
            registrations.AddRange(result.Variables);
            while (result.ContinuationToken != null) {
                result = await service.ListDataSetVariablesAsync(dataSetWriterId,
                    result.ContinuationToken, null, ct);
                registrations.AddRange(result.Variables);
            }
            return registrations;
        }
    }
}
