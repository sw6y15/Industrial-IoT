// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Services {
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Linq;

    /// <summary>
    /// Receive updates to writer group registry and update the engine as result.
    /// This encapsulates the work that happens between Publisher Registry, IoT Hub
    /// and Edge module, where the notifications change the twin state and cause
    /// the action here.
    /// </summary>
    public class WriterRegistryConnector : IWriterGroupRegistryListener,
        IDataSetWriterRegistryListener {

        public WriterRegistryConnector(IDataSetWriterRegistry registry,
            Func<IWriterGroupDataCollector> collectors, Func<IWriterGroupMessageEmitter> emitters,
            IPublisherEvents<IWriterGroupRegistryListener> b1,
            IPublisherEvents<IDataSetWriterRegistryListener> b2) {
            _registry = registry;
            _collectors = collectors;
            _emitters = emitters;

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
                writerGroupTwin.Activate(_collectors.Invoke(), _emitters.Invoke());
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
                    UpdateWriterGroupProcessor();
                }
            }

            public bool Activated => _collector != null;

            public HashSet<DataSetWriterModel> Writers { get; } = new HashSet<DataSetWriterModel>(
                Compare.Using<DataSetWriterModel>((a, b) => a.DataSetWriterId == b.DataSetWriterId));

            public void Activate(IWriterGroupDataCollector collector, IWriterGroupMessageEmitter emitter) {
                _collector = collector;
                _emitter = emitter;
                UpdateWriterGroupProcessor();
                _collector.AddWriters(Writers);
            }

            private void UpdateWriterGroupProcessor() {
                if (_collector == null) {
                    return;
                }

                // Apply now
                _emitter.WriterGroupId = _group.WriterGroupId;
                _emitter.MaxNetworkMessageSize = _group.MaxNetworkMessageSize;
                _emitter.BatchSize = _group.BatchSize;
                _emitter.PublishingInterval = _group.PublishingInterval;
                _emitter.Encoding = _group.Encoding;
                _emitter.Schema = _group.Schema;
                _emitter.HeaderLayoutUri = _group.HeaderLayoutUri;
                _emitter.DataSetOrdering = _group.MessageSettings?.DataSetOrdering;
                _emitter.MessageContentMask = _group.MessageSettings?.NetworkMessageContentMask;
                _emitter.PublishingOffset = _group.MessageSettings?.PublishingOffset?.ToList();

                _collector.Priority = _group.Priority;
                _collector.GroupVersion = _group.MessageSettings?.GroupVersion;
                _collector.KeepAliveTime = _group.KeepAliveTime;
                _collector.SamplingOffset = _group.MessageSettings?.SamplingOffset;
            }

            public void Deactivate() {
                // _engine.RemoveAllWriters();
                (_emitter as IDisposable).Dispose();
                (_collector as IDisposable).Dispose();
                _collector = null;
                _emitter = null;
            }

            public void AddWriter(DataSetWriterModel writer) {
                Writers.Remove(writer); // Remove and add to update
                Writers.Add(writer);
                if (_collector != null) {
                    _collector.AddWriters(writer.YieldReturn());
                }
            }

            public void RemoveWriter(string dataSetWriterId) {
                if (_collector != null) {
                    _collector.RemoveWriters(dataSetWriterId.YieldReturn());
                }
                Writers.RemoveWhere(w => w.DataSetWriterId == dataSetWriterId);
            }

            private IWriterGroupDataCollector _collector;
            private IWriterGroupMessageEmitter _emitter;
            private WriterGroupInfoModel _group;
        }

        private readonly ConcurrentDictionary<string, WriterGroupTwin> _twins =
            new ConcurrentDictionary<string, WriterGroupTwin>();
        private readonly IDataSetWriterRegistry _registry;
        private readonly Func<IWriterGroupDataCollector> _collectors;
        private readonly Func<IWriterGroupMessageEmitter> _emitters;
    }
}
