// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Registry.Models {
    using Microsoft.Azure.IIoT.Hub;
    using System;

    /// <summary>
    /// Service model extensions for writer group registry services
    /// </summary>
    public static class WriterGroupRegistryEx {

        /// <summary>
        /// Convert a writer Group Id to device id
        /// </summary>
        /// <param name="writerGroupId"></param>
        /// <returns></returns>
        public static string ToDeviceId(string writerGroupId) {
            return kDeviceIdPrefix + writerGroupId;
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
            if (deviceId.StartsWith(kDeviceIdPrefix)) {
                return deviceId.Substring(kDeviceIdPrefix.Length);
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

        private const string kDeviceIdPrefix = "job_";
    }
}
