// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher.Events.v2 {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Events.v2.Models;
    using Microsoft.Azure.IIoT.Messaging;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    /// <summary>
    /// Writer Group registry change listener
    /// </summary>
    public class WriterGroupEventBusSubscriber : IEventHandler<WriterGroupEventModel>,
        IDisposable {

        /// <summary>
        /// Create event subscriber
        /// </summary>
        /// <param name="bus"></param>
        /// <param name="listeners"></param>
        public WriterGroupEventBusSubscriber(IEventBus bus,
            IEnumerable<IWriterGroupRegistryListener> listeners) {
            _bus = bus ?? throw new ArgumentNullException(nameof(bus));
            _listeners = listeners?.ToList() ?? new List<IWriterGroupRegistryListener>();
            _token = _bus.RegisterAsync(this).Result;
        }

        /// <inheritdoc/>
        public void Dispose() {
            _bus.UnregisterAsync(_token).Wait();
        }

        /// <inheritdoc/>
        public async Task HandleAsync(WriterGroupEventModel eventData) {
            switch (eventData.EventType) {
                case WriterGroupEventType.Added:
                    await Task.WhenAll(_listeners
                        .Select(l => l.OnWriterGroupAddedAsync(
                            eventData.Context, eventData.WriterGroup)
                        .ContinueWith(t => Task.CompletedTask)));
                    break;
                case WriterGroupEventType.Updated:
                    await Task.WhenAll(_listeners
                        .Select(l => l.OnWriterGroupUpdatedAsync(
                            eventData.Context, eventData.WriterGroup)
                        .ContinueWith(t => Task.CompletedTask)));
                    break;
                case WriterGroupEventType.StateChange:
                    await Task.WhenAll(_listeners
                        .Select(l => l.OnWriterGroupStateChangeAsync(
                            eventData.Context, eventData.WriterGroup)
                        .ContinueWith(t => Task.CompletedTask)));
                    break;
                case WriterGroupEventType.Activated:
                    await Task.WhenAll(_listeners
                        .Select(l => l.OnWriterGroupActivatedAsync(
                            eventData.Context, eventData.WriterGroup)
                        .ContinueWith(t => Task.CompletedTask)));
                    break;
                case WriterGroupEventType.Deactivated:
                    await Task.WhenAll(_listeners
                        .Select(l => l.OnWriterGroupDeactivatedAsync(
                            eventData.Context, eventData.WriterGroup)
                        .ContinueWith(t => Task.CompletedTask)));
                    break;
                case WriterGroupEventType.Removed:
                    await Task.WhenAll(_listeners
                        .Select(l => l.OnWriterGroupRemovedAsync(
                            eventData.Context, eventData.Id)
                        .ContinueWith(t => Task.CompletedTask)));
                    break;
            }
        }

        private readonly IEventBus _bus;
        private readonly List<IWriterGroupRegistryListener> _listeners;
        private readonly string _token;
    }
}
