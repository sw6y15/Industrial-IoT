// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos {
    using Microsoft.Azure.IIoT.Storage;

    /// <summary>
    /// Operation options extension
    /// </summary>
    public static class OperationOptionsEx {

        /// <summary>
        /// Convert to request options
        /// </summary>
        /// <param name="options"></param>
        /// <param name="partitioned"></param>
        /// <returns></returns>
        public static PartitionKey ToPartitionKey(this OperationOptions options,
            bool partitioned = true) {
            return !partitioned || string.IsNullOrEmpty(options?.PartitionKey) ? default :
                new PartitionKey(options?.PartitionKey);
        }
    }
}
