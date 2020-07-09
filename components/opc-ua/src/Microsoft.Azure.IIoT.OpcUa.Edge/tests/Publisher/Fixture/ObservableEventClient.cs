// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Services {
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.Messaging;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;
    using System;
    using Microsoft.Azure.IIoT.Serializers;
    using Microsoft.Azure.IIoT.Serializers.NewtonSoft;

    /// <summary>
    /// Observe events emitted
    /// </summary>
    public class ObservableEventClient : IEventClient, IWriterGroupStateReporter {

        public void OnDataSetEventStateChange(string dataSetWriterId,
            PublishedDataSetItemStateModel state) {
            GetItemStates(dataSetWriterId, null).Events.TryAdd(state);
        }

        public void OnDataSetVariableStateChange(string dataSetWriterId, string variableId,
            PublishedDataSetItemStateModel state) {
            GetItemStates(dataSetWriterId, variableId).Events.TryAdd(state);
        }

        public void OnDataSetWriterStateChange(string dataSetWriterId,
            PublishedDataSetSourceStateModel state) {
            GetSourceStates(dataSetWriterId).Events.TryAdd(state);
        }

        public Task SendEventAsync(byte[] data, string contentType, string eventSchema,
            string contentEncoding, CancellationToken ct) {
            var message = new Message(data, contentType, eventSchema, contentEncoding);
            GetMessages(null).Events.TryAdd(message);
            return Task.CompletedTask;
        }

        public Task SendEventAsync(IEnumerable<byte[]> batch, string contentType,
            string eventSchema, string contentEncoding, CancellationToken ct) {
            foreach (var data in batch) {
                var message = new Message(data, contentType, eventSchema, contentEncoding);
                GetMessages(null).Events.TryAdd(message);
            }
            return Task.CompletedTask;
        }

        public EventStore<PublishedDataSetItemStateModel> GetItemStates(
            string dataSetWriterId, string variableId) {
            var key = dataSetWriterId + (variableId ?? "events");
            return _items.GetOrAdd(key, new EventStore<PublishedDataSetItemStateModel>());
        }

        public EventStore<PublishedDataSetSourceStateModel> GetSourceStates(string dataSetWriterId) {
            return _sources.GetOrAdd(dataSetWriterId, new EventStore<PublishedDataSetSourceStateModel>());
        }

        public EventStore<Message> GetMessages(string writerGroupId) {
            return _messages.GetOrAdd("todo", new EventStore<Message>());
        }

        /// <summary>
        /// Message wrapper
        /// </summary>
        public class Message {

            public Message(byte[] data, string contentType, string eventSchema, string contentEncoding) {
                Data = data;
                ContentType = contentType;
                EventSchema = eventSchema;
                ContentEncoding = contentEncoding;
            }

            public byte[] Data { get; }
            public string ContentType { get; }
            public string EventSchema { get; }
            public string ContentEncoding { get; }

            /// <summary>
            /// Decode json
            /// </summary>
            /// <returns></returns>
            public VariantValue Decode() {
                if (ContentType == ContentMimeType.Json) {
                    return _serializer.Parse(Data.AsMemory());
                }
                return VariantValue.Null;
            }

            public override string ToString() {
                if (ContentType == ContentMimeType.Json) {
                    return Decode().ToJson();
                }
                return EventSchema;
            }

            private readonly IJsonSerializer _serializer = new NewtonSoftJsonSerializer();
        }

        /// <summary>
        /// Helper
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public class EventStore<T> {

            public BlockingCollection<T> Events { get; } = new BlockingCollection<T>();

            public T WaitForEvent(Predicate<T> predicate, int timeout = 200000) {
                T result;
                while (Events.TryTake(out result, timeout)) {
                    if (predicate(result)) {
                        return result;
                    }
                }
                return default;
            }

            public T WaitForEvent(int timeout = 200000) {
                if (Events.TryTake(out var result, timeout)) {
                    return result;
                }
                return default;
            }
        }


        private readonly ConcurrentDictionary<string, EventStore<PublishedDataSetItemStateModel>> _items =
            new ConcurrentDictionary<string, EventStore<PublishedDataSetItemStateModel>>();
        private readonly ConcurrentDictionary<string, EventStore<PublishedDataSetSourceStateModel>> _sources =
            new ConcurrentDictionary<string, EventStore<PublishedDataSetSourceStateModel>>();
        private readonly ConcurrentDictionary<string, EventStore<Message>> _messages =
            new ConcurrentDictionary<string, EventStore<Message>>();
    }
}
