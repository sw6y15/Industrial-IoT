// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Registry.Handlers {
    using Microsoft.Azure.IIoT.OpcUa.Registry.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher;
    using Microsoft.Azure.IIoT.Hub;
    using Microsoft.Azure.IIoT.Hub.Models;
    using Microsoft.Azure.IIoT.Serializers;
    using System;
    using System.Threading.Tasks;

    /// <summary>
    /// Writer group event handler.
    /// </summary>
    public sealed class WriterGroupTwinEventHandler : IIoTHubDeviceTwinEventHandler {

        /// <summary>
        /// Create handler
        /// </summary>
        /// <param name="registry"></param>
        public WriterGroupTwinEventHandler(IWriterGroupStateUpdate registry) {
            _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        }

        /// <inheritdoc/>
        public async Task HandleDeviceTwinEventAsync(DeviceTwinEvent ev) {
            if (ev.Handled) {
                return;
            }

            var writerGroupId = WriterGroupRegistryEx.ToWriterGroupId(ev.Twin.Id);
            if (string.IsNullOrEmpty(writerGroupId)) {
                return;
            }

            // We only care about connectivity changes indicating activation state
            var type = ev.Twin.Tags?.GetValueOrDefault<string>(TwinProperty.Type, null);
            if (string.IsNullOrEmpty(type)) {
                return;
            }

            var context = new PublisherOperationContextModel {
                Time = ev.Timestamp,
                AuthorityId = ev.AuthorityId
            };
            if (IdentityType.WriterGroup.EqualsIgnoreCase(type)) {
                switch (ev.Event) {
                    case DeviceTwinEventType.Update:
                        // Toggle connection state
                        var state = ev.Twin.IsConnected() ?? false ?
                                WriterGroupState.Pending : WriterGroupState.Publishing;
                        await _registry.UpdateWriterGroupStateAsync(writerGroupId,
                            state, context);
                        break;
                    case DeviceTwinEventType.Delete:
                        // Writer group identity deleted - disable group state
                        await _registry.UpdateWriterGroupStateAsync(writerGroupId,
                            WriterGroupState.Disabled, context);
                        break;
                }
                ev.Handled = true;
            }
        }

        private readonly IWriterGroupStateUpdate _registry;
    }
}
