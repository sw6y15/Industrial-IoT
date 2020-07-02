// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Events.v2 {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Events.v2.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.Messaging;
    using System;
    using System.Threading.Tasks;

    /// <summary>
    /// Writer group registry event publisher
    /// </summary>
    public class WriterGroupEventBusPublisher : IWriterGroupRegistryListener {

        /// <summary>
        /// Create event publisher
        /// </summary>
        /// <param name="bus"></param>
        public WriterGroupEventBusPublisher(IEventBus bus) {
            _bus = bus ?? throw new ArgumentNullException(nameof(bus));
        }


        /// <inheritdoc/>
        public Task OnWriterGroupAddedAsync(PublisherOperationContextModel context,
            WriterGroupInfoModel writerGroup) {
            return _bus.PublishAsync(Wrap(WriterGroupEventType.Added, context,
                writerGroup.WriterGroupId, writerGroup));
        }

        /// <inheritdoc/>
        public Task OnWriterGroupUpdatedAsync(PublisherOperationContextModel context,
            WriterGroupInfoModel writerGroup) {
            return _bus.PublishAsync(Wrap(WriterGroupEventType.Updated, context,
                writerGroup.WriterGroupId, writerGroup));
        }

        /// <inheritdoc/>
        public Task OnWriterGroupStateChangeAsync(PublisherOperationContextModel context,
            WriterGroupInfoModel writerGroup) {
            return _bus.PublishAsync(Wrap(WriterGroupEventType.StateChange, context,
                writerGroup.WriterGroupId, writerGroup));
        }

        /// <inheritdoc/>
        public Task OnWriterGroupActivatedAsync(PublisherOperationContextModel context,
            WriterGroupInfoModel writerGroup) {
            return _bus.PublishAsync(Wrap(WriterGroupEventType.Activated, context,
                writerGroup.WriterGroupId, writerGroup));
        }

        /// <inheritdoc/>
        public Task OnWriterGroupDeactivatedAsync(PublisherOperationContextModel context,
            WriterGroupInfoModel writerGroup) {
            return _bus.PublishAsync(Wrap(WriterGroupEventType.Deactivated, context,
                writerGroup.WriterGroupId, writerGroup));
        }

        /// <inheritdoc/>
        public Task OnWriterGroupRemovedAsync(PublisherOperationContextModel context,
            string writerGroupId) {
            return _bus.PublishAsync(Wrap(WriterGroupEventType.Removed, context,
                writerGroupId, null));
        }

        /// <summary>
        /// Create writer group event
        /// </summary>
        /// <param name="type"></param>
        /// <param name="context"></param>
        /// <param name="writerGroupId"></param>
        /// <param name="writerGroup"></param>
        /// <returns></returns>
        private static WriterGroupEventModel Wrap(WriterGroupEventType type,
            PublisherOperationContextModel context, string writerGroupId,
            WriterGroupInfoModel writerGroup) {
            return new WriterGroupEventModel {
                EventType = type,
                Context = context,
                Id = writerGroupId,
                WriterGroup = writerGroup
            };
        }

        private readonly IEventBus _bus;
    }
}
