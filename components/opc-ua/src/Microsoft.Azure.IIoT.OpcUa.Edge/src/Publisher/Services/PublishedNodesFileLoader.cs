// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Services {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.Crypto;
    using Microsoft.Azure.IIoT.Serializers;
    using Microsoft.Azure.IIoT.Utils;
    using Serilog;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Security.Cryptography;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.IIoT.OpcUa.Core;

    /// <summary>
    /// Loads published nodes file and configures the engine
    /// </summary>
    public class PublishedNodesFileLoader : IHostProcess, IDisposable {

        /// <summary>
        /// Create published nodes file loader
        /// </summary>
        /// <param name="engine"></param>
        /// <param name="serializer"></param>
        /// <param name="legacyCliModel"></param>
        /// <param name="logger"></param>
        /// <param name="cryptoProvider"></param>
        public PublishedNodesFileLoader(IWriterGroupProcessingEngine engine,
            IJsonSerializer serializer, ILegacyCliModelProvider legacyCliModel,
            ILogger logger, ISecureElement cryptoProvider = null) {

            _engine = engine ?? throw new ArgumentNullException(nameof(engine));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _lastSetOfWriterIds = new HashSet<string>();

            _file = new PublishedNodesFile(serializer, legacyCliModel,
                logger, cryptoProvider);
            if (string.IsNullOrWhiteSpace(_file.FileName)) {
                throw new ArgumentNullException(nameof(_file.FileName));
            }

            _diagnosticInterval =
                legacyCliModel?.LegacyCliModel?.DiagnosticsInterval;
            _messagingMode =
                legacyCliModel?.LegacyCliModel?.Schema ?? MessageSchema.Samples;
            _messageEncoding =
                legacyCliModel?.LegacyCliModel?.Encoding ?? MessageEncoding.Json;

            var directory = Path.GetDirectoryName(_file.FileName);
            var file = Path.GetFileName(_file.FileName);
            if (string.IsNullOrWhiteSpace(directory)) {
                directory = Environment.CurrentDirectory;
            }
            _fileSystemWatcher = new FileSystemWatcher(directory, file);
        }

        /// <inheritdoc/>
        public Task StartAsync() {
            _engine.DiagnosticsInterval = _diagnosticInterval;

            OnPublishedNodesFileChanged(null, null); // load first time

            _fileSystemWatcher.Changed += OnPublishedNodesFileChanged;
            _fileSystemWatcher.EnableRaisingEvents = true;

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public Task StopAsync() {
            _fileSystemWatcher.EnableRaisingEvents = false;
            _fileSystemWatcher.Changed -= OnPublishedNodesFileChanged;

            // Remove all current writers stopping writing messages
            _engine.RemoveAllWriters();

            _engine.DiagnosticsInterval = null;
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public void Dispose() {
            _fileSystemWatcher.Dispose();
            Try.Op(_engine.RemoveAllWriters);
            // Engine is also stopped
        }

        /// <summary>
        /// Called on change
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void OnPublishedNodesFileChanged(object sender, FileSystemEventArgs e) {
            var retryCount = 3;
            while (true) {
                try {
                    var currentFileHash = GetChecksum(_file.FileName);
                    if (currentFileHash != _lastKnownFileHash) {
                        _logger.Information("File {fileName} has changed, reloading...",
                            _file.FileName);
                        _lastKnownFileHash = currentFileHash;
                        var group = _file.Read();

                        group.DataSetWriters.ForEach(d => {
                            d.DataSet.ExtensionFields ??= new Dictionary<string, string>();
                            d.DataSet.ExtensionFields["DataSetWriterId"] = d.DataSetWriterId;
                        });

                        // Update engine under lock
                        lock (_fileLock) {
                            _engine.Priority = group.Priority;
                            _engine.BatchSize = group.BatchSize;
                            _engine.PublishingInterval = group.PublishingInterval;
                            _engine.DataSetOrdering = group.MessageSettings?.DataSetOrdering;
                            _engine.GroupVersion = group.MessageSettings?.GroupVersion;
                            _engine.HeaderLayoutUri = group.HeaderLayoutUri;
                            _engine.KeepAliveTime = group.KeepAliveTime;
                            _engine.MaxNetworkMessageSize = group.MaxNetworkMessageSize;

                            _engine.MessageSchema = MessageSchemaEx.ToMessageSchemaMimeType(
                                group.Schema, group.Encoding);

                            _engine.NetworkMessageContentMask =
                                group.MessageSettings?.NetworkMessageContentMask;
                            _engine.PublishingOffset =
                                group.MessageSettings?.PublishingOffset?.ToList();
                            _engine.SamplingOffset =
                                group.MessageSettings?.SamplingOffset;

                            var dataSetWriterIds = group?.DataSetWriters?
                                .Select(w => w.DataSetWriterId)
                                .ToHashSet() ?? new HashSet<string>();
                            _lastSetOfWriterIds.ExceptWith(dataSetWriterIds);
                            _engine.RemoveWriters(_lastSetOfWriterIds);
                            _engine.AddWriters(group.DataSetWriters);
                            _lastSetOfWriterIds = dataSetWriterIds;
                        }
                    }
                    break; // Success
                }
                catch (IOException ex) {
                    retryCount--;
                    if (retryCount > 0) {
                        _logger.Debug("Error while loading job from file, retrying...");
                    }
                    else {
                        _logger.Error(ex,
                            "Error while loading job from file. Retry expired, giving up.");
                        break;
                    }
                }
            }
        }

        /// <summary>
        /// Get a checksum for the current file
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        private static string GetChecksum(string file) {
            using (var stream = File.OpenRead(file))
            using (var sha = new SHA256Managed())  {
                var checksum = sha.ComputeHash(stream);
                return BitConverter.ToString(checksum);
            }
        }

        private readonly FileSystemWatcher _fileSystemWatcher;
        private readonly IWriterGroupProcessingEngine _engine;
        private readonly PublishedNodesFile _file;
        private readonly ILogger _logger;
        private readonly object _fileLock = new object();
        private readonly TimeSpan? _diagnosticInterval;
        private readonly MessageSchema _messagingMode;
        private readonly MessageEncoding _messageEncoding;
        private string _lastKnownFileHash;
        private HashSet<string> _lastSetOfWriterIds;
    }
}