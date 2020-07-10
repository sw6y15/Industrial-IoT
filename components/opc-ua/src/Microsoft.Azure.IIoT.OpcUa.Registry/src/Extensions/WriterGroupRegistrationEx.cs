// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Registry.Models {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using Microsoft.Azure.IIoT.Hub;
    using Microsoft.Azure.IIoT.Hub.Models;
    using Microsoft.Azure.IIoT.Serializers;
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Writer group registration extensions
    /// </summary>
    public static class WriterGroupRegistrationEx {

        /// <summary>
        /// Create device twin
        /// </summary>
        /// <param name="registration"></param>
        /// <param name="serializer"></param>
        /// <returns></returns>
        public static DeviceTwinModel ToDeviceTwin(this WriterGroupRegistration registration,
            IJsonSerializer serializer) {
            return Patch(null, registration, serializer);
        }

        /// <summary>
        /// Create patch twin model to upload
        /// </summary>
        /// <param name="existing"></param>
        /// <param name="update"></param>
        /// <param name="serializer"></param>
        public static DeviceTwinModel Patch(this WriterGroupRegistration existing,
            WriterGroupRegistration update, IJsonSerializer serializer) {

            var twin = new DeviceTwinModel {
                Id = (existing?.DeviceId ?? update?.DeviceId)
                    ?? throw new ArgumentException("DeviceId must not be null"),
                Etag = existing?.Etag,
                Status = update?.IsDisabled == null ? null :
                    update.IsDisabled.Value ? "disabled" : "enabled",
                Tags = new Dictionary<string, VariantValue>(),
                Properties = new TwinPropertiesModel {
                    Desired = new Dictionary<string, VariantValue>()
                }
            };

            // Tags

            if (update?.IsDisabled != null &&
                update.IsDisabled != existing?.IsDisabled) {
                twin.Tags.Add(nameof(EntityRegistration.IsDisabled), (update?.IsDisabled ?? false) ?
                    true : (bool?)null);
                twin.Tags.Add(nameof(EntityRegistration.NotSeenSince), (update?.IsDisabled ?? false) ?
                    DateTime.UtcNow : (DateTime?)null);
            }

            if (update?.WriterGroupId != null &&
                update.WriterGroupId != existing?.WriterGroupId) {
                twin.Tags.Add(nameof(WriterGroupRegistration.WriterGroupId), update.WriterGroupId);
                twin.Id = PublisherRegistryEx.ToDeviceId(update.WriterGroupId);
            }

            if (update?.SiteId != existing?.SiteId) {
                twin.Tags.Add(nameof(WriterGroupRegistration.SiteId), update?.SiteId);
            }

            twin.Tags.Add(nameof(EntityRegistration.DeviceType), update?.DeviceType);

            // Group Property

            if (update?.Schema != existing?.Schema) {
                twin.Properties.Desired.Add(nameof(WriterGroupRegistration.Schema),
                    update?.Schema == null ?
                        null : serializer.FromObject(update.Schema.ToString()));
            }

            if (update?.Encoding != existing?.Encoding) {
                twin.Properties.Desired.Add(nameof(WriterGroupRegistration.Encoding),
                    update?.Encoding == null ?
                        null : serializer.FromObject(update.Encoding.ToString()));
            }

            if (update?.NetworkMessageContentMask != existing?.NetworkMessageContentMask) {
                twin.Properties.Desired.Add(nameof(WriterGroupRegistration.NetworkMessageContentMask),
                    update?.NetworkMessageContentMask == null ?
                        null : serializer.FromObject(update.NetworkMessageContentMask.ToString()));
            }

            if (update?.DataSetOrdering != existing?.DataSetOrdering) {
                twin.Properties.Desired.Add(nameof(WriterGroupRegistration.DataSetOrdering),
                    update?.DataSetOrdering == null ?
                        null : serializer.FromObject(update.DataSetOrdering.ToString()));
            }

            if (update?.HeaderLayoutUri != existing?.HeaderLayoutUri) {
                twin.Properties.Desired.Add(nameof(WriterGroupRegistration.HeaderLayoutUri),
                    update?.HeaderLayoutUri);
            }

            if (update?.SamplingOffset != existing?.SamplingOffset) {
                twin.Properties.Desired.Add(nameof(WriterGroupRegistration.SamplingOffset),
                    update?.SamplingOffset);
            }

            if (update?.BatchSize != existing?.BatchSize) {
                twin.Properties.Desired.Add(nameof(WriterGroupRegistration.BatchSize),
                    update?.BatchSize);
            }

            if (update?.Priority != existing?.Priority) {
                twin.Properties.Desired.Add(nameof(WriterGroupRegistration.Priority),
                    update?.Priority);
            }

            if (update?.KeepAliveTime != existing?.KeepAliveTime) {
                twin.Properties.Desired.Add(nameof(WriterGroupRegistration.KeepAliveTime),
                    update?.KeepAliveTime);
            }

            if (update?.PublishingInterval != existing?.PublishingInterval) {
                twin.Properties.Desired.Add(nameof(WriterGroupRegistration.PublishingInterval),
                    update?.PublishingInterval);
            }

            if (update?.GroupVersion != existing?.GroupVersion) {
                twin.Properties.Desired.Add(nameof(WriterGroupRegistration.GroupVersion),
                    update?.GroupVersion);
            }

            if (update?.MaxNetworkMessageSize != existing?.MaxNetworkMessageSize) {
                twin.Properties.Desired.Add(nameof(WriterGroupRegistration.MaxNetworkMessageSize),
                    update?.MaxNetworkMessageSize);
            }

            var offsetsEqual = update?.PublishingOffset.DecodeAsList().ToHashSetSafe().SetEqualsSafe(
                existing?.PublishingOffset?.DecodeAsList());
            if (!(offsetsEqual ?? true)) {
                twin.Properties.Desired.Add(nameof(WriterGroupRegistration.PublishingOffset),
                    update?.PublishingOffset == null ?
                    null : serializer.FromObject(update.PublishingOffset));
            }

            var localesEqual = update?.LocaleIds.DecodeAsList().ToHashSetSafe().SetEqualsSafe(
                existing?.LocaleIds?.DecodeAsList());
            if (!(localesEqual ?? true)) {
                twin.Properties.Desired.Add(nameof(WriterGroupRegistration.LocaleIds),
                    update?.LocaleIds == null ?
                    null : serializer.FromObject(update.LocaleIds));
            }
            return twin;
        }

        /// <summary>
        /// Decode tags and property into registration object
        /// </summary>
        /// <param name="twin"></param>
        /// <param name="properties"></param>
        /// <returns></returns>
        public static WriterGroupRegistration ToWriterGroupRegistration(this DeviceTwinModel twin,
            Dictionary<string, VariantValue> properties) {
            if (twin == null) {
                return null;
            }

            var tags = twin.Tags ?? new Dictionary<string, VariantValue>();

            var registration = new WriterGroupRegistration {
                // Device

                DeviceId = twin.Id,
                Etag = twin.Etag,
                Version = null,
                Connected = twin.IsConnected() ?? false,

                // Tags
                IsDisabled =
                    tags.GetValueOrDefault(nameof(WriterGroupRegistration.IsDisabled), twin.IsDisabled()),
                NotSeenSince =
                    tags.GetValueOrDefault<DateTime>(nameof(WriterGroupRegistration.NotSeenSince), null),
                WriterGroupId =
                    tags.GetValueOrDefault<string>(nameof(WriterGroupRegistration.WriterGroupId), null),
                SiteId =
                    tags.GetValueOrDefault<string>(nameof(WriterGroupRegistration.SiteId), null),

                // Properties

                Type =
                    properties.GetValueOrDefault<string>(TwinProperty.Type, null),
                BatchSize =
                    properties.GetValueOrDefault<int>(nameof(WriterGroupRegistration.BatchSize), null),
                MaxNetworkMessageSize =
                    properties.GetValueOrDefault<uint>(nameof(WriterGroupRegistration.MaxNetworkMessageSize), null),
                GroupVersion =
                    properties.GetValueOrDefault<uint>(nameof(WriterGroupRegistration.GroupVersion), null),
                SamplingOffset =
                    properties.GetValueOrDefault<double>(nameof(WriterGroupRegistration.SamplingOffset), null),
                PublishingInterval =
                    properties.GetValueOrDefault<TimeSpan>(nameof(WriterGroupRegistration.PublishingInterval), null),
                KeepAliveTime =
                    properties.GetValueOrDefault<TimeSpan>(nameof(WriterGroupRegistration.KeepAliveTime), null),
                DataSetOrdering =
                    properties.GetValueOrDefault<DataSetOrderingType>(nameof(WriterGroupRegistration.DataSetOrdering), null),
                PublishingOffset =
                    properties.GetValueOrDefault<Dictionary<string, double>>(nameof(WriterGroupRegistration.PublishingOffset), null),
                LocaleIds =
                    properties.GetValueOrDefault<Dictionary<string, string>>(nameof(WriterGroupRegistration.LocaleIds), null),
                Priority =
                    properties.GetValueOrDefault<byte>(nameof(WriterGroupRegistration.Priority), null),
                NetworkMessageContentMask =
                    properties.GetValueOrDefault<NetworkMessageContentMask>(nameof(WriterGroupRegistration.NetworkMessageContentMask), null),
                Schema =
                    properties.GetValueOrDefault<MessageSchema>(nameof(WriterGroupRegistration.Schema), null),
                Encoding =
                    properties.GetValueOrDefault<MessageEncoding>(nameof(WriterGroupRegistration.Encoding), null),
                HeaderLayoutUri =
                    properties.GetValueOrDefault<string>(nameof(WriterGroupRegistration.HeaderLayoutUri), null)
            };
            return registration;
        }

        /// <summary>
        /// Make sure to get the registration information from the right place.
        /// Reported (truth) properties take precedence over desired. However,
        /// if there is nothing reported, it means the endpoint is not currently
        /// serviced, thus we use desired as if they are attributes of the
        /// endpoint.
        /// </summary>
        /// <param name="twin"></param>
        /// <param name="onlyServerState">Only desired endpoint should be returned
        /// this means that you will look at stale information.</param>
        /// <returns></returns>
        public static WriterGroupRegistration ToWriterGroupRegistration(this DeviceTwinModel twin,
            bool onlyServerState) {

            if (twin == null) {
                return null;
            }
            if (twin.Tags == null) {
                twin.Tags = new Dictionary<string, VariantValue>();
            }

            var consolidated =
                ToWriterGroupRegistration(twin, twin.GetConsolidatedProperties());
            var desired = (twin.Properties?.Desired == null) ? null :
                ToWriterGroupRegistration(twin, twin.Properties.Desired);

            if (!onlyServerState) {
                consolidated._isInSync = consolidated.IsInSyncWith(desired);
                return consolidated;
            }
            if (desired != null) {
                desired._isInSync = desired.IsInSyncWith(consolidated);
            }
            return desired;
        }

        /// <summary>
        /// Decode tags and property into registration object
        /// </summary>
        /// <param name="model"></param>
        /// <param name="disabled"></param>
        /// <returns></returns>
        public static WriterGroupRegistration ToWriterGroupRegistration(this WriterGroupInfoModel model,
            bool? disabled = null) {
            if (model == null) {
                throw new ArgumentNullException(nameof(model));
            }
            return new WriterGroupRegistration {
                IsDisabled = disabled,
                WriterGroupId = model.WriterGroupId,
                DeviceId = PublisherRegistryEx.ToDeviceId(model.WriterGroupId),
                SiteId = model.SiteId,
                BatchSize = model.BatchSize,
                PublishingInterval = model.PublishingInterval,
                DataSetOrdering = model.MessageSettings?.DataSetOrdering,
                GroupVersion = model.MessageSettings?.GroupVersion,
                HeaderLayoutUri = model.HeaderLayoutUri,
                KeepAliveTime = model.KeepAliveTime,
                LocaleIds = model.LocaleIds?.EncodeAsDictionary(),
                MaxNetworkMessageSize = model.MaxNetworkMessageSize,
                Schema = model.Schema,
                Encoding = model.Encoding,
                Priority = model.Priority,
                NetworkMessageContentMask = model.MessageSettings?.NetworkMessageContentMask,
                PublishingOffset = model.MessageSettings?.PublishingOffset?.EncodeAsDictionary(),
                SamplingOffset = model.MessageSettings?.SamplingOffset
            };
        }

        /// <summary>
        /// Convert to service model
        /// </summary>
        /// <param name="registration"></param>
        /// <returns></returns>
        public static WriterGroupInfoModel ToServiceModel(this WriterGroupRegistration registration) {
            if (registration == null) {
                return null;
            }
            return new WriterGroupInfoModel {
                BatchSize = registration.BatchSize,
                PublishingInterval = registration.PublishingInterval,
                HeaderLayoutUri = registration.HeaderLayoutUri,
                WriterGroupId = registration.WriterGroupId,
                KeepAliveTime = registration.KeepAliveTime,
                LocaleIds = registration.LocaleIds.DecodeAsList(),
                MaxNetworkMessageSize = registration.MaxNetworkMessageSize,
                Schema = registration.Schema,
                Encoding = registration.Encoding,
                Priority = registration.Priority,
                SiteId = registration.SiteId,
                MessageSettings = new WriterGroupMessageSettingsModel {
                    DataSetOrdering = registration.DataSetOrdering,
                    GroupVersion = registration.GroupVersion,
                    NetworkMessageContentMask = registration.NetworkMessageContentMask,
                    PublishingOffset = registration.PublishingOffset.DecodeAsList(),
                    SamplingOffset = registration.SamplingOffset
                }
            };
        }

        /// <summary>
        /// Convert to status model
        /// </summary>
        /// <param name="registration"></param>
        /// <returns></returns>
        public static EntityActivationStatusModel ToStatusModel(this WriterGroupRegistration registration) {
            if (registration == null) {
                return null;
            }
            return new EntityActivationStatusModel {
                Id = registration.WriterGroupId,
                ActivationState = registration.Connected ?
                    EntityActivationState.ActivatedAndConnected : EntityActivationState.Activated
            };
        }

        /// <summary>
        /// Flag endpoint as synchronized - i.e. it matches the other.
        /// </summary>
        /// <param name="registration"></param>
        /// <param name="other"></param>
        internal static bool IsInSyncWith(this WriterGroupRegistration registration,
            WriterGroupRegistration other) {
            if (registration == null) {
                return other == null;
            }
            if (other.Schema != registration.Schema) {
                return false;
            }
            if (other.Encoding != registration.Encoding) {
                return false;
            }
            if (other.KeepAliveTime != registration.KeepAliveTime) {
                return false;
            }
            if (other.PublishingInterval != registration.PublishingInterval) {
                return false;
            }
            if (other.MaxNetworkMessageSize != registration.MaxNetworkMessageSize) {
                return false;
            }
            if (other.HeaderLayoutUri != registration.HeaderLayoutUri) {
                return false;
            }
            if (other.SamplingOffset != registration.SamplingOffset) {
                return false;
            }
            if (other.DataSetOrdering != registration.DataSetOrdering) {
                return false;
            }
            if (other.NetworkMessageContentMask != registration.NetworkMessageContentMask) {
                return false;
            }
            if (other.Priority != registration.Priority) {
                return false;
            }
            if (other.GroupVersion != registration.GroupVersion) {
                return false;
            }
            if (other.BatchSize != registration.BatchSize) {
                return false;
            }
            if (!other.LocaleIds.DecodeAsList().SetEqualsSafe(
                    registration.LocaleIds.DecodeAsList(), (x, y) => x == y)) {
                return false;
            }
            if (!other.PublishingOffset.DecodeAsList().SetEqualsSafe(
                    registration.PublishingOffset.DecodeAsList(), (x, y) => x == y)) {
                return false;
            }
            return true;
        }
    }
}
