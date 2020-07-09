// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Services {
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Linq;
    using Xunit;
    using System.Collections.Concurrent;

    /// <summary>
    /// Receive updates to writer group registry and update the engine as result.
    /// This encapsulates the work that happens between Publisher Registry, IoT Hub
    /// and Edge module, where the notifications change the twin state and cause
    /// the action here.
    /// </summary>
    public class WriterRegistryEngineConnector : IWriterGroupRegistryListener,
        IDataSetWriterRegistryListener {

        public WriterRegistryEngineConnector(IDataSetWriterRegistry registry,
            Func<IWriterGroupProcessingEngine> engine,
            IPublisherEvents<IWriterGroupRegistryListener> b1,
            IPublisherEvents<IDataSetWriterRegistryListener> b2) {
            _registry = registry;
            _engine = engine;

            b1.Register(this);
            b2.Register(this);
        }

        public async Task OnDataSetWriterAddedAsync(PublisherOperationContextModel context,
            DataSetWriterInfoModel dataSetWriter) {
            // Same as what the edge module does remotely
            if (_twins.TryGetValue(dataSetWriter.WriterGroupId, out var writerGroupTwin)) {
                var writer = await _registry.GetDataSetWriterAsync(dataSetWriter.DataSetWriterId);
                writerGroupTwin.AddWriter(writer);
            }
        }

        public Task OnDataSetWriterRemovedAsync(PublisherOperationContextModel context,
            DataSetWriterInfoModel dataSetWriter) {
            // Same as what the edge module does remotely
            if (_twins.TryGetValue(dataSetWriter.WriterGroupId, out var writerGroupTwin)) {
                writerGroupTwin.RemoveWriter(dataSetWriter.DataSetWriterId);
            }
            return Task.CompletedTask;
        }

        public Task OnDataSetWriterStateChangeAsync(PublisherOperationContextModel context,
            string dataSetWriterId, DataSetWriterInfoModel dataSetWriter) {
            // No op
            return Task.CompletedTask;
        }

        public async Task OnDataSetWriterUpdatedAsync(PublisherOperationContextModel context,
            string dataSetWriterId, DataSetWriterInfoModel dataSetWriter) {
            // Same as what the edge module does remotely
            var writer = await _registry.GetDataSetWriterAsync(dataSetWriterId);
            foreach (var writerGroupTwin in _twins.Values
                .Where(v => v.Writers.Any(w => w.DataSetWriterId == dataSetWriterId))) {
                writerGroupTwin.AddWriter(writer);
            }
        }

        public Task OnWriterGroupAddedAsync(PublisherOperationContextModel context,
            WriterGroupInfoModel writerGroup) {
            _twins.TryAdd(writerGroup.WriterGroupId, new WriterGroupTwin {
                Group = writerGroup
            });
            return Task.CompletedTask;
        }

        public Task OnWriterGroupUpdatedAsync(PublisherOperationContextModel context,
            WriterGroupInfoModel writerGroup) {
            if (_twins.TryGetValue(writerGroup.WriterGroupId, out var writerGroupTwin)) {
                writerGroupTwin.Group = writerGroup;
            }
            return Task.CompletedTask;
        }

        public Task OnWriterGroupActivatedAsync(PublisherOperationContextModel context,
            WriterGroupInfoModel writerGroup) {
            if (_twins.TryGetValue(writerGroup.WriterGroupId, out var writerGroupTwin)) {
                writerGroupTwin.Activate(_engine.Invoke());
            }
            return Task.CompletedTask;
        }

        public Task OnWriterGroupDeactivatedAsync(PublisherOperationContextModel context,
            WriterGroupInfoModel writerGroup) {
            if (_twins.TryGetValue(writerGroup.WriterGroupId, out var writerGroupTwin)) {
                writerGroupTwin.Deactivate();
            }
            return Task.CompletedTask;
        }

        public Task OnWriterGroupRemovedAsync(PublisherOperationContextModel context,
            string writerGroupId) {
            _twins.TryRemove(writerGroupId, out _);
            return Task.CompletedTask;
        }

        public Task OnWriterGroupStateChangeAsync(PublisherOperationContextModel context,
            WriterGroupInfoModel writerGroup) {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Collects the content of writer group information and applies it to
        /// a new engine instance on activation.  Kills the engine on deactivation.
        /// </summary>
        private class WriterGroupTwin {

            public string WriterGroupId => _group.WriterGroupId;

            public WriterGroupInfoModel Group {
                get => _group;
                set {
                    _group = value.Clone();
                    UpdateEngine();
                }
            }

            public bool Activated => _engine != null;

            public HashSet<DataSetWriterModel> Writers { get; } = new HashSet<DataSetWriterModel>(
                Compare.Using<DataSetWriterModel>((a, b) => a.DataSetWriterId == b.DataSetWriterId));

            public void Activate(IWriterGroupProcessingEngine engine) {
                _engine = engine;
                UpdateEngine();
                _engine.AddWriters(Writers);
            }

            private void UpdateEngine() {
                if (_engine == null) {
                    return;
                }
                // Apply now
                _engine.Priority = _group.Priority;
                _engine.BatchSize = _group.BatchSize;
                _engine.PublishingInterval = _group.PublishingInterval;
                _engine.DataSetOrdering = _group.MessageSettings?.DataSetOrdering;
                _engine.GroupVersion = _group.MessageSettings?.GroupVersion;
                _engine.HeaderLayoutUri = _group.HeaderLayoutUri;
                _engine.KeepAliveTime = _group.KeepAliveTime;
                _engine.MaxNetworkMessageSize = _group.MaxNetworkMessageSize;
                _engine.MessageSchema = MessageSchemaEx.ToMessageSchemaMimeType(_group.Schema, _group.Encoding);
                _engine.NetworkMessageContentMask = _group.MessageSettings?.NetworkMessageContentMask;
                _engine.PublishingOffset = _group.MessageSettings?.PublishingOffset?.ToList();
                _engine.SamplingOffset = _group.MessageSettings?.SamplingOffset;
            }

            public void Deactivate() {
                // _engine.RemoveAllWriters();
                (_engine as IDisposable).Dispose();
                _engine = null;
            }

            public void AddWriter(DataSetWriterModel writer) {
                Writers.Remove(writer); // Remove and add to update
                Writers.Add(writer);
                if (_engine != null) {
                    _engine.AddWriters(writer.YieldReturn());
                }
            }

            public void RemoveWriter(string dataSetWriterId) {
                if (_engine != null) {
                    _engine.RemoveWriters(dataSetWriterId.YieldReturn());
                }
                Writers.RemoveWhere(w => w.DataSetWriterId == dataSetWriterId);
            }

            private IWriterGroupProcessingEngine _engine;
            private WriterGroupInfoModel _group;
        }

        private readonly ConcurrentDictionary<string, WriterGroupTwin> _twins =
            new ConcurrentDictionary<string, WriterGroupTwin>();
        private readonly IDataSetWriterRegistry _registry;
        private readonly Func<IWriterGroupProcessingEngine> _engine;
    }
}
