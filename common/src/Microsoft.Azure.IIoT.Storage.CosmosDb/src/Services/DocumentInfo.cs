// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.Storage.CosmosDb.Services {
    using Microsoft.Azure.Cosmos;
    using System;

    /// <summary>
    /// Document wrapper
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal sealed class DocumentInfo<T> : IDocumentInfo<T> {

        /// <summary>
        /// Create document
        /// </summary>
        /// <param name="doc"></param>
        internal DocumentInfo(ItemResponse<T> doc) {
            _doc = doc ?? throw new ArgumentNullException(nameof(doc));
        }

        /// <inheritdoc/>
        public string Id => ((dynamic)_doc.Resource).Id;

        /// <inheritdoc/>
        public T Value => _doc.Resource;

        /// <inheritdoc/>
        public string PartitionKey => _doc.GetPropertyValue<string>(
            DocumentCollection.PartitionKeyProperty);

        /// <inheritdoc/>
        public string Etag => _doc.ETag;

        private readonly ItemResponse<T> _doc;
    }
}
