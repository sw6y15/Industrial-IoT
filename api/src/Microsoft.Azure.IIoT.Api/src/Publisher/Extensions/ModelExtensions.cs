// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Api.Publisher.Models {
    using Microsoft.Azure.IIoT.OpcUa.Api.Core.Models;
    using Microsoft.Azure.IIoT.OpcUa.Core.Models;
    using Microsoft.Azure.IIoT.OpcUa.Subscriber.Models;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Models;
    using System.Linq;

    /// <summary>
    /// Api model extensions
    /// </summary>
    public static class ModelExtensions {

        /// <summary>
        /// Convert to api model
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public static MonitoredItemMessageApiModel ToApiModel(
            this MonitoredItemMessageModel model) {
            if (model == null) {
                return null;
            }
            return new MonitoredItemMessageApiModel {
                PublisherId = model.PublisherId,
                DataSetWriterId = model.DataSetWriterId,
                EndpointId = model.EndpointId,
                NodeId = model.NodeId,
                DisplayName = model.DisplayName,
                ServerTimestamp = model.ServerTimestamp,
                ServerPicoseconds = model.ServerPicoseconds,
                SourceTimestamp = model.SourceTimestamp,
                SourcePicoseconds = model.SourcePicoseconds,
                Timestamp = model.Timestamp,
                Value = model.Value?.Copy(),
                DataType = model.DataType,
                Status = model.Status
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static ConfigurationVersionApiModel ToApiModel(
            this ConfigurationVersionModel model) {
            if (model == null) {
                return null;
            }
            return new ConfigurationVersionApiModel {
                MajorVersion = model.MajorVersion,
                MinorVersion = model.MinorVersion
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static ConfigurationVersionModel ToServiceModel(
            this ConfigurationVersionApiModel model) {
            if (model == null) {
                return null;
            }
            return new ConfigurationVersionModel {
                MajorVersion = model.MajorVersion,
                MinorVersion = model.MinorVersion
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static ConnectionApiModel ToApiModel(
            this ConnectionModel model) {
            if (model == null) {
                return null;
            }
            return new ConnectionApiModel {
                Endpoint = model.Endpoint.ToApiModel(),
                User = model.User.ToApiModel(),
                Diagnostics = model.Diagnostics.ToApiModel()
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static ConnectionModel ToServiceModel(
            this ConnectionApiModel model) {
            if (model == null) {
                return null;
            }
            return new ConnectionModel {
                Endpoint = model.Endpoint.ToServiceModel(),
                User = model.User.ToServiceModel(),
                Diagnostics = model.Diagnostics.ToServiceModel()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static ContentFilterApiModel ToApiModel(
            this ContentFilterModel model) {
            if (model == null) {
                return null;
            }
            return new ContentFilterApiModel {
                Elements = model.Elements?
                    .Select(e => e.ToApiModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static ContentFilterModel ToServiceModel(
            this ContentFilterApiModel model) {
            if (model == null) {
                return null;
            }
            return new ContentFilterModel {
                Elements = model.Elements?
                    .Select(e => e.ToServiceModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static ContentFilterElementApiModel ToApiModel(
            this ContentFilterElementModel model) {
            if (model == null) {
                return null;
            }
            return new ContentFilterElementApiModel {
                FilterOperands = model.FilterOperands?
                    .Select(f => f.ToApiModel())
                    .ToList(),
                FilterOperator = (Core.Models.FilterOperatorType)model.FilterOperator
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static ContentFilterElementModel ToServiceModel(
            this ContentFilterElementApiModel model) {
            if (model == null) {
                return null;
            }
            return new ContentFilterElementModel {
                FilterOperands = model.FilterOperands?
                    .Select(f => f.ToServiceModel())
                    .ToList(),
                FilterOperator = (OpcUa.Core.Models.FilterOperatorType)model.FilterOperator
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static CredentialApiModel ToApiModel(
            this CredentialModel model) {
            if (model == null) {
                return null;
            }
            return new CredentialApiModel {
                Value = model.Value,
                Type = (Core.Models.CredentialType?)model.Type
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static CredentialModel ToServiceModel(
            this CredentialApiModel model) {
            if (model == null) {
                return null;
            }
            return new CredentialModel {
                Value = model.Value,
                Type = (OpcUa.Core.Models.CredentialType?)model.Type
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static DataSetMetaDataApiModel ToApiModel(
            this DataSetMetaDataModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetMetaDataApiModel {
                Name = model.Name,
                ConfigurationVersion = model.ConfigurationVersion.ToApiModel(),
                DataSetClassId = model.DataSetClassId,
                Description = model.Description.ToApiModel(),
                Fields = model.Fields?
                    .Select(f => f.ToApiModel())
                    .ToList(),
                EnumDataTypes = model.EnumDataTypes?
                    .Select(f => f.ToApiModel())
                    .ToList(),
                StructureDataTypes = model.StructureDataTypes?
                    .Select(f => f.ToApiModel())
                    .ToList(),
                SimpleDataTypes = model.SimpleDataTypes?
                    .Select(f => f.ToApiModel())
                    .ToList(),
                Namespaces = model.Namespaces?.ToList()
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static DataSetMetaDataModel ToServiceModel(
            this DataSetMetaDataApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetMetaDataModel {
                Name = model.Name,
                ConfigurationVersion = model.ConfigurationVersion.ToServiceModel(),
                DataSetClassId = model.DataSetClassId,
                Description = model.Description.ToServiceModel(),
                Fields = model.Fields?
                    .Select(f => f.ToServiceModel())
                    .ToList(),
                EnumDataTypes = model.EnumDataTypes?
                    .Select(f => f.ToServiceModel())
                    .ToList(),
                StructureDataTypes = model.StructureDataTypes?
                    .Select(f => f.ToServiceModel())
                    .ToList(),
                SimpleDataTypes = model.SimpleDataTypes?
                    .Select(f => f.ToServiceModel())
                    .ToList(),
                Namespaces = model.Namespaces?.ToList()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static DataSetWriterApiModel ToApiModel(
            this DataSetWriterModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterApiModel {
                GenerationId = model.GenerationId,
                DataSetWriterId = model.DataSetWriterId,
                DataSet = model.DataSet.ToApiModel(),
                DataSetFieldContentMask = (DataSetFieldContentMask?)model.DataSetFieldContentMask,
                DataSetMetaDataSendInterval = model.DataSetMetaDataSendInterval,
                KeyFrameCount = model.KeyFrameCount,
                KeyFrameInterval = model.KeyFrameInterval,
                MessageSettings = model.MessageSettings.ToApiModel()
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static DataSetWriterModel ToServiceModel(
            this DataSetWriterApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterModel {
                GenerationId = model.GenerationId,
                DataSetWriterId = model.DataSetWriterId,
                DataSet = model.DataSet.ToServiceModel(),
                DataSetFieldContentMask = (OpcUa.Publisher.Models.DataSetFieldContentMask?)model.DataSetFieldContentMask,
                DataSetMetaDataSendInterval = model.DataSetMetaDataSendInterval,
                KeyFrameCount = model.KeyFrameCount,
                KeyFrameInterval = model.KeyFrameInterval,
                MessageSettings = model.MessageSettings.ToServiceModel()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static DataSetWriterMessageSettingsApiModel ToApiModel(
            this DataSetWriterMessageSettingsModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterMessageSettingsApiModel {
                ConfiguredSize = model.ConfiguredSize,
                DataSetMessageContentMask = (DataSetContentMask?)model.DataSetMessageContentMask,
                DataSetOffset = model.DataSetOffset,
                NetworkMessageNumber = model.NetworkMessageNumber
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static DataSetWriterMessageSettingsModel ToServiceModel(
            this DataSetWriterMessageSettingsApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterMessageSettingsModel {
                ConfiguredSize = model.ConfiguredSize,
                DataSetMessageContentMask = (OpcUa.Publisher.Models.DataSetContentMask?)model.DataSetMessageContentMask,
                DataSetOffset = model.DataSetOffset,
                NetworkMessageNumber = model.NetworkMessageNumber
            };
        }

        /// <summary>
        /// Create from service model
        /// </summary>
        /// <param name="model"></param>
        public static DiagnosticsApiModel ToApiModel(
            this DiagnosticsModel model) {
            if (model == null) {
                return null;
            }
            return new DiagnosticsApiModel {
                AuditId = model.AuditId,
                Level = (Core.Models.DiagnosticsLevel?)model.Level,
                TimeStamp = model.TimeStamp
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static DiagnosticsModel ToServiceModel(
            this DiagnosticsApiModel model) {
            if (model == null) {
                return null;
            }
            return new DiagnosticsModel {
                AuditId = model.AuditId,
                Level = (OpcUa.Core.Models.DiagnosticsLevel?)model.Level,
                TimeStamp = model.TimeStamp
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static EndpointApiModel ToApiModel(
            this EndpointModel model) {
            if (model == null) {
                return null;
            }
            return new EndpointApiModel {
                Url = model.Url,
                AlternativeUrls = model.AlternativeUrls,
                Certificate = model.Certificate,
                SecurityMode = (Core.Models.SecurityMode?)model.SecurityMode,
                SecurityPolicy = model.SecurityPolicy
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static EndpointModel ToServiceModel(
            this EndpointApiModel model) {
            if (model == null) {
                return null;
            }
            return new EndpointModel {
                Url = model.Url,
                AlternativeUrls = model.AlternativeUrls,
                Certificate = model.Certificate,
                SecurityMode = (OpcUa.Core.Models.SecurityMode?)model.SecurityMode,
                SecurityPolicy = model.SecurityPolicy
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static EnumDefinitionApiModel ToApiModel(
            this EnumDefinitionModel model) {
            if (model == null) {
                return null;
            }
            return new EnumDefinitionApiModel {
                Fields = model.Fields?
                    .Select(f => f.ToApiModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static EnumDefinitionModel ToServiceModel(
            this EnumDefinitionApiModel model) {
            if (model == null) {
                return null;
            }
            return new EnumDefinitionModel {
                Fields = model.Fields?
                    .Select(f => f.ToServiceModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static EnumDescriptionApiModel ToApiModel(
            this EnumDescriptionModel model) {
            if (model == null) {
                return null;
            }
            return new EnumDescriptionApiModel {
                Name = model.Name,
                BuiltInType = model.BuiltInType,
                DataTypeId = model.DataTypeId,
                EnumDefinition = model.EnumDefinition.ToApiModel()
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static EnumDescriptionModel ToServiceModel(
            this EnumDescriptionApiModel model) {
            if (model == null) {
                return null;
            }
            return new EnumDescriptionModel {
                Name = model.Name,
                BuiltInType = model.BuiltInType,
                DataTypeId = model.DataTypeId,
                EnumDefinition = model.EnumDefinition.ToServiceModel()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static EnumFieldApiModel ToApiModel(
            this EnumFieldModel model) {
            if (model == null) {
                return null;
            }
            return new EnumFieldApiModel {
                Name = model.Name,
                Description = model.Description.ToApiModel(),
                DisplayName = model.DisplayName.ToApiModel(),
                Value = model.Value
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static EnumFieldModel ToServiceModel(
            this EnumFieldApiModel model) {
            if (model == null) {
                return null;
            }
            return new EnumFieldModel {
                Name = model.Name,
                Description = model.Description.ToServiceModel(),
                DisplayName = model.DisplayName.ToServiceModel(),
                Value = model.Value
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static FieldMetaDataApiModel ToApiModel(
            this FieldMetaDataModel model) {
            if (model == null) {
                return null;
            }
            return new FieldMetaDataApiModel {
                Description = model.Description.ToApiModel(),
                ArrayDimensions = model.ArrayDimensions?.ToList(),
                BuiltInType = model.BuiltInType,
                DataSetFieldId = model.DataSetFieldId,
                DataTypeId = model.DataTypeId,
                FieldFlags = model.FieldFlags,
                MaxStringLength = model.MaxStringLength,
                Name = model.Name,
                Properties = model.Properties?
                    .ToDictionary(k => k.Key, v => v.Value),
                ValueRank = model.ValueRank
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static FieldMetaDataModel ToServiceModel(
            this FieldMetaDataApiModel model) {
            if (model == null) {
                return null;
            }
            return new FieldMetaDataModel {
                Description = model.Description.ToServiceModel(),
                ArrayDimensions = model.ArrayDimensions?.ToList(),
                BuiltInType = model.BuiltInType,
                DataSetFieldId = model.DataSetFieldId,
                DataTypeId = model.DataTypeId,
                FieldFlags = model.FieldFlags,
                MaxStringLength = model.MaxStringLength,
                Name = model.Name,
                Properties = model.Properties?
                    .ToDictionary(k => k.Key, v => v.Value),
                ValueRank = model.ValueRank
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static FilterOperandApiModel ToApiModel(
            this FilterOperandModel model) {
            if (model == null) {
                return null;
            }
            return new FilterOperandApiModel {
                Index = model.Index,
                Alias = model.Alias,
                Value = model.Value,
                NodeId = model.NodeId,
                AttributeId = (Core.Models.NodeAttribute?)model.AttributeId,
                BrowsePath = model.BrowsePath,
                IndexRange = model.IndexRange
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static FilterOperandModel ToServiceModel(
            this FilterOperandApiModel model) {
            if (model == null) {
                return null;
            }
            return new FilterOperandModel {
                Index = model.Index,
                Alias = model.Alias,
                Value = model.Value,
                NodeId = model.NodeId,
                AttributeId = (OpcUa.Core.Models.NodeAttribute?)model.AttributeId,
                BrowsePath = model.BrowsePath,
                IndexRange = model.IndexRange
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static LocalizedTextApiModel ToApiModel(
            this LocalizedTextModel model) {
            if (model == null) {
                return null;
            }
            return new LocalizedTextApiModel {
                Locale = model.Locale,
                Text = model.Text
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static LocalizedTextModel ToServiceModel(
            this LocalizedTextApiModel model) {
            if (model == null) {
                return null;
            }
            return new LocalizedTextModel {
                Locale = model.Locale,
                Text = model.Text
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static PublishedDataItemsApiModel ToApiModel(
            this PublishedDataItemsModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataItemsApiModel {
                PublishedData = model.PublishedData?
                    .Select(d => d.ToApiModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static PublishedDataItemsModel ToServiceModel(
            this PublishedDataItemsApiModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataItemsModel {
                PublishedData = model.PublishedData?
                    .Select(d => d.ToServiceModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static PublishedDataSetApiModel ToApiModel(
            this PublishedDataSetModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetApiModel {
                Name = model.Name,
                DataSetSource = model.DataSetSource.ToApiModel(),
                DataSetMetaData = model.DataSetMetaData.ToApiModel(),
                ExtensionFields = model.ExtensionFields?
                    .ToDictionary(k => k.Key, v => v.Value)
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static PublishedDataSetModel ToServiceModel(
            this PublishedDataSetApiModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetModel {
                Name = model.Name,
                DataSetSource = model.DataSetSource.ToServiceModel(),
                DataSetMetaData = model.DataSetMetaData.ToServiceModel(),
                ExtensionFields = model.ExtensionFields?
                    .ToDictionary(k => k.Key, v => v.Value)
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static PublishedDataSetItemStateApiModel ToApiModel(
            this PublishedDataSetItemStateModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetItemStateApiModel {
                LastResult = model.LastResult.ToApiModel(),
                LastResultChange = model.LastResultChange,
                ClientId = model.ClientId,
                ServerId = model.ServerId,
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static PublishedDataSetItemStateModel ToServiceModel(
            this PublishedDataSetItemStateApiModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetItemStateModel {
                LastResult = model.LastResult.ToServiceModel(),
                LastResultChange = model.LastResultChange,
                ClientId = model.ClientId,
                ServerId = model.ServerId,
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static PublishedDataSetEventsApiModel ToApiModel(
            this PublishedDataSetEventsModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetEventsApiModel {
                Id = model.Id,
                State = model.State.ToApiModel(),
                GenerationId = model.GenerationId,
                Created = model.Created.ToApiModel(),
                Updated = model.Updated.ToApiModel(),
                DiscardNew = model.DiscardNew,
                EventNotifier = model.EventNotifier,
                BrowsePath = model.BrowsePath,
                Filter = model.Filter.ToApiModel(),
                QueueSize = model.QueueSize,
                MonitoringMode = (MonitoringMode?)model.MonitoringMode,
                TriggerId = model.TriggerId,
                SelectedFields = model.SelectedFields?
                    .Select(f => f.ToApiModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static PublishedDataSetEventsModel ToServiceModel(
            this PublishedDataSetEventsApiModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetEventsModel {
                Id = model.Id,
                State = model.State.ToServiceModel(),
                GenerationId = model.GenerationId,
                Created = model.Created.ToServiceModel(),
                Updated = model.Updated.ToServiceModel(),
                DiscardNew = model.DiscardNew,
                EventNotifier = model.EventNotifier,
                BrowsePath = model.BrowsePath,
                Filter = model.Filter.ToServiceModel(),
                QueueSize = model.QueueSize,
                MonitoringMode = (OpcUa.Publisher.Models.MonitoringMode?)model.MonitoringMode,
                TriggerId = model.TriggerId,
                SelectedFields = model.SelectedFields?
                    .Select(f => f.ToServiceModel())
                    .ToList()
            };
        }


        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static PublishedDataSetSourceSettingsApiModel ToApiModel(
            this PublishedDataSetSourceSettingsModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetSourceSettingsApiModel {
                LifeTimeCount = model.LifeTimeCount,
                MaxKeepAliveCount = model.MaxKeepAliveCount,
                MaxNotificationsPerPublish = model.MaxNotificationsPerPublish,
                Priority = model.Priority,
                ResolveDisplayName = model.ResolveDisplayName,
                PublishingInterval = model.PublishingInterval
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static PublishedDataSetSourceSettingsModel ToServiceModel(
            this PublishedDataSetSourceSettingsApiModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetSourceSettingsModel {
                LifeTimeCount = model.LifeTimeCount,
                MaxKeepAliveCount = model.MaxKeepAliveCount,
                MaxNotificationsPerPublish = model.MaxNotificationsPerPublish,
                Priority = model.Priority,
                ResolveDisplayName = model.ResolveDisplayName,
                PublishingInterval = model.PublishingInterval
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static PublishedDataSetSourceApiModel ToApiModel(
            this PublishedDataSetSourceModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetSourceApiModel {
                Connection = model.Connection.ToApiModel(),
                State = model.State.ToApiModel(),
                PublishedEvents = model.PublishedEvents.ToApiModel(),
                PublishedVariables = model.PublishedVariables.ToApiModel(),
                SubscriptionSettings = model.SubscriptionSettings.ToApiModel()
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static PublishedDataSetSourceModel ToServiceModel(
            this PublishedDataSetSourceApiModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetSourceModel {
                Connection = model.Connection.ToServiceModel(),
                State = model.State.ToServiceModel(),
                PublishedEvents = model.PublishedEvents.ToServiceModel(),
                PublishedVariables = model.PublishedVariables.ToServiceModel(),
                SubscriptionSettings = model.SubscriptionSettings.ToServiceModel()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static PublishedDataSetVariableApiModel ToApiModel(
            this PublishedDataSetVariableModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetVariableApiModel {
                Id = model.Id,
                State = model.State.ToApiModel(),
                GenerationId = model.GenerationId,
                Created = model.Created.ToApiModel(),
                Updated = model.Updated.ToApiModel(),
                Order = model.Order,
                PublishedVariableNodeId = model.PublishedVariableNodeId,
                Attribute = (Core.Models.NodeAttribute?)model.Attribute,
                DataChangeFilter = (DataChangeTriggerType?)model.DataChangeFilter,
                DeadbandType = (DeadbandType?)model.DeadbandType,
                DeadbandValue = model.DeadbandValue,
                DiscardNew = model.DiscardNew,
                BrowsePath = model.BrowsePath,
                IndexRange = model.IndexRange,
                MonitoringMode = (MonitoringMode?)model.MonitoringMode,
                MetaDataProperties = model.MetaDataProperties?.ToList(),
                QueueSize = model.QueueSize,
                SamplingInterval = model.SamplingInterval,
                TriggerId = model.TriggerId,
                HeartbeatInterval = model.HeartbeatInterval,
                PublishedVariableDisplayName = model.PublishedVariableDisplayName,
                SubstituteValue = model.SubstituteValue?.Copy()
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static PublishedDataSetVariableModel ToServiceModel(
            this PublishedDataSetVariableApiModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetVariableModel {
                Id = model.Id,
                State = model.State.ToServiceModel(),
                GenerationId = model.GenerationId,
                Created = model.Created.ToServiceModel(),
                Updated = model.Updated.ToServiceModel(),
                Order = model.Order,
                PublishedVariableNodeId = model.PublishedVariableNodeId,
                Attribute = (OpcUa.Core.Models.NodeAttribute?)model.Attribute,
                DataChangeFilter = (OpcUa.Publisher.Models.DataChangeTriggerType?)model.DataChangeFilter,
                DeadbandType = (OpcUa.Publisher.Models.DeadbandType?)model.DeadbandType,
                DeadbandValue = model.DeadbandValue,
                DiscardNew = model.DiscardNew,
                BrowsePath = model.BrowsePath,
                IndexRange = model.IndexRange,
                MonitoringMode = (OpcUa.Publisher.Models.MonitoringMode?)model.MonitoringMode,
                MetaDataProperties = model.MetaDataProperties?
                    .ToList(),
                QueueSize = model.QueueSize,
                SamplingInterval = model.SamplingInterval,
                TriggerId = model.TriggerId,
                HeartbeatInterval = model.HeartbeatInterval,
                PublishedVariableDisplayName = model.PublishedVariableDisplayName,
                SubstituteValue = model.SubstituteValue?.Copy()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static SimpleAttributeOperandApiModel ToApiModel(
            this SimpleAttributeOperandModel model) {
            if (model == null) {
                return null;
            }
            return new SimpleAttributeOperandApiModel {
                NodeId = model.NodeId,
                AttributeId = (Core.Models.NodeAttribute?)model.AttributeId,
                BrowsePath = model.BrowsePath,
                IndexRange = model.IndexRange
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static SimpleAttributeOperandModel ToServiceModel(
            this SimpleAttributeOperandApiModel model) {
            if (model == null) {
                return null;
            }
            return new SimpleAttributeOperandModel {
                NodeId = model.NodeId,
                AttributeId = (OpcUa.Core.Models.NodeAttribute?)model.AttributeId,
                BrowsePath = model.BrowsePath,
                IndexRange = model.IndexRange
            };
        }


        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static SimpleTypeDescriptionApiModel ToApiModel(
            this SimpleTypeDescriptionModel model) {
            if (model == null) {
                return null;
            }
            return new SimpleTypeDescriptionApiModel {
                BaseDataTypeId = model.BaseDataTypeId,
                Name = model.Name,
                DataTypeId = model.DataTypeId,
                BuiltInType = model.BuiltInType
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static SimpleTypeDescriptionModel ToServiceModel(
            this SimpleTypeDescriptionApiModel model) {
            if (model == null) {
                return null;
            }
            return new SimpleTypeDescriptionModel {
                BaseDataTypeId = model.BaseDataTypeId,
                Name = model.Name,
                DataTypeId = model.DataTypeId,
                BuiltInType = model.BuiltInType
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static StructureDefinitionApiModel ToApiModel(
            this StructureDefinitionModel model) {
            if (model == null) {
                return null;
            }
            return new StructureDefinitionApiModel {
                BaseDataTypeId = model.BaseDataTypeId,
                Fields = model.Fields?
                    .Select(f => f.ToApiModel())
                    .ToList(),
                StructureType = (Core.Models.StructureType)model.StructureType
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static StructureDefinitionModel ToServiceModel(
            this StructureDefinitionApiModel model) {
            if (model == null) {
                return null;
            }
            return new StructureDefinitionModel {
                BaseDataTypeId = model.BaseDataTypeId,
                Fields = model.Fields?
                    .Select(f => f.ToServiceModel())
                    .ToList(),
                StructureType = (OpcUa.Core.Models.StructureType)model.StructureType
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static StructureDescriptionApiModel ToApiModel(
            this StructureDescriptionModel model) {
            if (model == null) {
                return null;
            }
            return new StructureDescriptionApiModel {
                DataTypeId = model.DataTypeId,
                Name = model.Name,
                StructureDefinition = model.StructureDefinition.ToApiModel()
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static StructureDescriptionModel ToServiceModel(
            this StructureDescriptionApiModel model) {
            if (model == null) {
                return null;
            }
            return new StructureDescriptionModel {
                DataTypeId = model.DataTypeId,
                Name = model.Name,
                StructureDefinition = model.StructureDefinition.ToServiceModel()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static StructureFieldApiModel ToApiModel(
            this StructureFieldModel model) {
            if (model == null) {
                return null;
            }
            return new StructureFieldApiModel {
                ArrayDimensions = model.ArrayDimensions?.ToList(),
                DataTypeId = model.DataTypeId,
                Description = model.Description.ToApiModel(),
                IsOptional = model.IsOptional,
                MaxStringLength = model.MaxStringLength,
                Name = model.Name,
                ValueRank = (Core.Models.NodeValueRank?)model.ValueRank
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static StructureFieldModel ToServiceModel(
            this StructureFieldApiModel model) {
            if (model == null) {
                return null;
            }
            return new StructureFieldModel {
                ArrayDimensions = model.ArrayDimensions?.ToList(),
                DataTypeId = model.DataTypeId,
                Description = model.Description.ToServiceModel(),
                IsOptional = model.IsOptional,
                MaxStringLength = model.MaxStringLength,
                Name = model.Name,
                ValueRank = (OpcUa.Core.Models.NodeValueRank?)model.ValueRank
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static WriterGroupApiModel ToApiModel(
            this WriterGroupModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupApiModel {
                GenerationId = model.GenerationId,
                WriterGroupId = model.WriterGroupId,
                BatchSize = model.BatchSize,
                SiteId = model.SiteId,
                HeaderLayoutUri = model.HeaderLayoutUri,
                KeepAliveTime = model.KeepAliveTime,
                LocaleIds = model.LocaleIds?.ToList(),
                MaxNetworkMessageSize = model.MaxNetworkMessageSize,
                MessageSettings = model.MessageSettings.ToApiModel(),
                MessageType = (NetworkMessageType?)model.MessageType,
                Name = model.Name,
                Priority = model.Priority,
                SecurityGroupId = model.SecurityGroupId,
                SecurityKeyServices = model.SecurityKeyServices?
                    .Select(s => s.ToApiModel())
                    .ToList(),
                DataSetWriters = model.DataSetWriters?
                    .Select(s => s.ToApiModel())
                    .ToList(),
                PublishingInterval = model.PublishingInterval,
                SecurityMode = (Core.Models.SecurityMode?)model.SecurityMode
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static WriterGroupModel ToServiceModel(
            this WriterGroupApiModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupModel {
                GenerationId = model.GenerationId,
                WriterGroupId = model.WriterGroupId,
                BatchSize = model.BatchSize,
                SiteId = model.SiteId,
                HeaderLayoutUri = model.HeaderLayoutUri,
                KeepAliveTime = model.KeepAliveTime,
                LocaleIds = model.LocaleIds?.ToList(),
                MaxNetworkMessageSize = model.MaxNetworkMessageSize,
                MessageSettings = model.MessageSettings.ToServiceModel(),
                MessageType = (OpcUa.Publisher.Models.NetworkMessageType?)model.MessageType,
                Name = model.Name,
                Priority = model.Priority,
                SecurityGroupId = model.SecurityGroupId,
                SecurityKeyServices = model.SecurityKeyServices?
                    .Select(s => s.ToServiceModel())
                    .ToList(),
                DataSetWriters = model.DataSetWriters?
                    .Select(s => s.ToServiceModel())
                    .ToList(),
                PublishingInterval = model.PublishingInterval,
                SecurityMode = (OpcUa.Core.Models.SecurityMode?)model.SecurityMode
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static WriterGroupInfoApiModel ToApiModel(
            this WriterGroupInfoModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupInfoApiModel {
                State = model.State.ToApiModel(),
                WriterGroupId = model.WriterGroupId,
                HeaderLayoutUri = model.HeaderLayoutUri,
                KeepAliveTime = model.KeepAliveTime,
                LocaleIds = model.LocaleIds?.ToList(),
                MaxNetworkMessageSize = model.MaxNetworkMessageSize,
                MessageSettings = model.MessageSettings.ToApiModel(),
                MessageType = (NetworkMessageType?)model.MessageType,
                Name = model.Name,
                Priority = model.Priority,
                SecurityGroupId = model.SecurityGroupId,
                SecurityKeyServices = model.SecurityKeyServices?
                    .Select(s => s.ToApiModel())
                    .ToList(),
                PublishingInterval = model.PublishingInterval,
                BatchSize = model.BatchSize,
                GenerationId = model.GenerationId,
                Created = model.Created.ToApiModel(),
                Updated = model.Updated.ToApiModel(),
                SiteId = model.SiteId,
                SecurityMode = (Core.Models.SecurityMode?)model.SecurityMode
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static WriterGroupInfoModel ToServiceModel(
            this WriterGroupInfoApiModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupInfoModel {
                State = model.State.ToServiceModel(),
                WriterGroupId = model.WriterGroupId,
                HeaderLayoutUri = model.HeaderLayoutUri,
                KeepAliveTime = model.KeepAliveTime,
                LocaleIds = model.LocaleIds?.ToList(),
                MaxNetworkMessageSize = model.MaxNetworkMessageSize,
                MessageSettings = model.MessageSettings.ToServiceModel(),
                MessageType = (OpcUa.Publisher.Models.NetworkMessageType?)model.MessageType,
                Name = model.Name,
                Priority = model.Priority,
                SecurityGroupId = model.SecurityGroupId,
                SecurityKeyServices = model.SecurityKeyServices?
                    .Select(s => s.ToServiceModel())
                    .ToList(),
                PublishingInterval = model.PublishingInterval,
                BatchSize = model.BatchSize,
                GenerationId = model.GenerationId,
                Created = model.Created.ToServiceModel(),
                Updated = model.Updated.ToServiceModel(),
                SiteId = model.SiteId,
                SecurityMode = (OpcUa.Core.Models.SecurityMode?)model.SecurityMode
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static WriterGroupMessageSettingsApiModel ToApiModel(
            this WriterGroupMessageSettingsModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupMessageSettingsApiModel {
                NetworkMessageContentMask = (NetworkMessageContentMask?)model.NetworkMessageContentMask,
                DataSetOrdering = (DataSetOrderingType?)model.DataSetOrdering,
                GroupVersion = model.GroupVersion,
                PublishingOffset = model.PublishingOffset,
                SamplingOffset = model.SamplingOffset
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static WriterGroupMessageSettingsModel ToServiceModel(
            this WriterGroupMessageSettingsApiModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupMessageSettingsModel {
                NetworkMessageContentMask = (OpcUa.Publisher.Models.NetworkMessageContentMask?)model.NetworkMessageContentMask,
                DataSetOrdering = (OpcUa.Publisher.Models.DataSetOrderingType?)model.DataSetOrdering,
                GroupVersion = model.GroupVersion,
                PublishingOffset = model.PublishingOffset,
                SamplingOffset = model.SamplingOffset
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetAddEventRequestApiModel ToApiModel(
            this DataSetAddEventRequestModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetAddEventRequestApiModel {
                DiscardNew = model.DiscardNew,
                EventNotifier = model.EventNotifier,
                BrowsePath = model.BrowsePath,
                Filter = model.Filter.ToApiModel(),
                QueueSize = model.QueueSize,
                MonitoringMode = (MonitoringMode?)model.MonitoringMode,
                TriggerId = model.TriggerId,
                SelectedFields = model.SelectedFields?
                    .Select(f => f.ToApiModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetAddEventRequestModel ToServiceModel(
            this DataSetAddEventRequestApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetAddEventRequestModel {
                DiscardNew = model.DiscardNew,
                EventNotifier = model.EventNotifier,
                BrowsePath = model.BrowsePath,
                Filter = model.Filter.ToServiceModel(),
                QueueSize = model.QueueSize,
                MonitoringMode = (OpcUa.Publisher.Models.MonitoringMode?)model.MonitoringMode,
                TriggerId = model.TriggerId,
                SelectedFields = model.SelectedFields?
                    .Select(f => f.ToServiceModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetAddEventResponseApiModel ToApiModel(
            this DataSetAddEventResultModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetAddEventResponseApiModel {
                GenerationId = model.GenerationId
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetAddEventResultModel ToServiceModel(
            this DataSetAddEventResponseApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetAddEventResultModel {
                GenerationId = model.GenerationId
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetAddVariableBatchRequestApiModel ToApiModel(
            this DataSetAddVariableBatchRequestModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetAddVariableBatchRequestApiModel {
                DataSetPublishingInterval = model.DataSetPublishingInterval,
                User = model.User.ToApiModel(),
                Variables = model.Variables?
                    .Select(r => r.ToApiModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetAddVariableBatchRequestModel ToServiceModel(
            this DataSetAddVariableBatchRequestApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetAddVariableBatchRequestModel {
                DataSetPublishingInterval = model.DataSetPublishingInterval,
                User = model.User.ToServiceModel(),
                Variables = model.Variables?
                    .Select(r => r.ToServiceModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetAddVariableBatchResponseApiModel ToApiModel(
            this DataSetAddVariableBatchResultModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetAddVariableBatchResponseApiModel {
                Results = model.Results?
                    .Select(r => r.ToApiModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetAddVariableBatchResultModel ToServiceModel(
            this DataSetAddVariableBatchResponseApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetAddVariableBatchResultModel {
                Results = model.Results?
                    .Select(r => r.ToServiceModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetAddVariableRequestApiModel ToApiModel(
            this DataSetAddVariableRequestModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetAddVariableRequestApiModel {
                Order = model.Order,
                PublishedVariableNodeId = model.PublishedVariableNodeId,
                Attribute = (Core.Models.NodeAttribute?)model.Attribute,
                DataChangeFilter = (DataChangeTriggerType?)model.DataChangeFilter,
                DeadbandType = (DeadbandType?)model.DeadbandType,
                DeadbandValue = model.DeadbandValue,
                DiscardNew = model.DiscardNew,
                BrowsePath = model.BrowsePath,
                IndexRange = model.IndexRange,
                MonitoringMode = (MonitoringMode?)model.MonitoringMode,
                MetaDataProperties = model.MetaDataProperties?
                    .ToList(),
                QueueSize = model.QueueSize,
                SamplingInterval = model.SamplingInterval,
                TriggerId = model.TriggerId,
                HeartbeatInterval = model.HeartbeatInterval,
                PublishedVariableDisplayName = model.PublishedVariableDisplayName,
                SubstituteValue = model.SubstituteValue?.Copy()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetAddVariableRequestModel ToServiceModel(
            this DataSetAddVariableRequestApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetAddVariableRequestModel {
                Order = model.Order,
                PublishedVariableNodeId = model.PublishedVariableNodeId,
                Attribute = (OpcUa.Core.Models.NodeAttribute?)model.Attribute,
                DataChangeFilter = (OpcUa.Publisher.Models.DataChangeTriggerType?)model.DataChangeFilter,
                DeadbandType = (OpcUa.Publisher.Models.DeadbandType?)model.DeadbandType,
                DeadbandValue = model.DeadbandValue,
                DiscardNew = model.DiscardNew,
                BrowsePath = model.BrowsePath,
                IndexRange = model.IndexRange,
                MonitoringMode = (OpcUa.Publisher.Models.MonitoringMode?)model.MonitoringMode,
                MetaDataProperties = model.MetaDataProperties?
                    .ToList(),
                QueueSize = model.QueueSize,
                SamplingInterval = model.SamplingInterval,
                TriggerId = model.TriggerId,
                HeartbeatInterval = model.HeartbeatInterval,
                PublishedVariableDisplayName = model.PublishedVariableDisplayName,
                SubstituteValue = model.SubstituteValue?.Copy()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetAddVariableResponseApiModel ToApiModel(
            this DataSetAddVariableResultModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetAddVariableResponseApiModel {
                GenerationId = model.GenerationId,
                Id = model.Id,
                ErrorInfo = model.ErrorInfo.ToApiModel()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetAddVariableResultModel ToServiceModel(
            this DataSetAddVariableResponseApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetAddVariableResultModel {
                GenerationId = model.GenerationId,
                Id = model.Id,
                ErrorInfo = model.ErrorInfo.ToServiceModel()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetRemoveVariableBatchRequestApiModel ToApiModel(
            this DataSetRemoveVariableBatchRequestModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetRemoveVariableBatchRequestApiModel {
                Variables = model.Variables?
                    .Select(r => r.ToApiModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetRemoveVariableBatchRequestModel ToServiceModel(
            this DataSetRemoveVariableBatchRequestApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetRemoveVariableBatchRequestModel {
                Variables = model.Variables?
                    .Select(r => r.ToServiceModel())
                    .ToList()
            };
        }


        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetRemoveVariableBatchResponseApiModel ToApiModel(
            this DataSetRemoveVariableBatchResultModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetRemoveVariableBatchResponseApiModel {
                Results = model.Results?
                    .Select(r => r.ToApiModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetRemoveVariableBatchResultModel ToServiceModel(
            this DataSetRemoveVariableBatchResponseApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetRemoveVariableBatchResultModel {
                Results = model.Results?
                    .Select(r => r.ToServiceModel())
                    .ToList()
            };
        }


        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetRemoveVariableRequestApiModel ToApiModel(
            this DataSetRemoveVariableRequestModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetRemoveVariableRequestApiModel {
                PublishedVariableNodeId = model.PublishedVariableNodeId
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetRemoveVariableRequestModel ToServiceModel(
            this DataSetRemoveVariableRequestApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetRemoveVariableRequestModel {
                PublishedVariableNodeId = model.PublishedVariableNodeId
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetRemoveVariableResponseApiModel ToApiModel(
            this DataSetRemoveVariableResultModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetRemoveVariableResponseApiModel {
                ErrorInfo = model.ErrorInfo.ToApiModel()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetRemoveVariableResultModel ToServiceModel(
            this DataSetRemoveVariableResponseApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetRemoveVariableResultModel {
                ErrorInfo = model.ErrorInfo.ToServiceModel()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetUpdateEventRequestApiModel ToApiModel(
            this DataSetUpdateEventRequestModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetUpdateEventRequestApiModel {
                GenerationId = model.GenerationId,
                DiscardNew = model.DiscardNew,
                Filter = model.Filter.ToApiModel(),
                QueueSize = model.QueueSize,
                MonitoringMode = (MonitoringMode?)model.MonitoringMode,
                TriggerId = model.TriggerId,
                SelectedFields = model.SelectedFields?
                    .Select(f => f.ToApiModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetUpdateEventRequestModel ToServiceModel(
            this DataSetUpdateEventRequestApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetUpdateEventRequestModel {
                GenerationId = model.GenerationId,
                DiscardNew = model.DiscardNew,
                Filter = model.Filter.ToServiceModel(),
                QueueSize = model.QueueSize,
                MonitoringMode = (OpcUa.Publisher.Models.MonitoringMode?)model.MonitoringMode,
                TriggerId = model.TriggerId,
                SelectedFields = model.SelectedFields?
                    .Select(f => f.ToServiceModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetUpdateVariableRequestApiModel ToApiModel(
            this DataSetUpdateVariableRequestModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetUpdateVariableRequestApiModel {
                GenerationId = model.GenerationId,
                DataChangeFilter = (DataChangeTriggerType?)model.DataChangeFilter,
                DeadbandType = (DeadbandType?)model.DeadbandType,
                DeadbandValue = model.DeadbandValue,
                DiscardNew = model.DiscardNew,
                MonitoringMode = (MonitoringMode?)model.MonitoringMode,
                QueueSize = model.QueueSize,
                SamplingInterval = model.SamplingInterval,
                TriggerId = model.TriggerId,
                HeartbeatInterval = model.HeartbeatInterval,
                PublishedVariableDisplayName = model.PublishedVariableDisplayName,
                SubstituteValue = model.SubstituteValue?.Copy()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetUpdateVariableRequestModel ToServiceModel(
            this DataSetUpdateVariableRequestApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetUpdateVariableRequestModel {
                GenerationId = model.GenerationId,
                DataChangeFilter = (OpcUa.Publisher.Models.DataChangeTriggerType ?)model.DataChangeFilter,
                DeadbandType = (OpcUa.Publisher.Models.DeadbandType?)model.DeadbandType,
                DeadbandValue = model.DeadbandValue,
                DiscardNew = model.DiscardNew,
                MonitoringMode = (OpcUa.Publisher.Models.MonitoringMode?)model.MonitoringMode,
                QueueSize = model.QueueSize,
                SamplingInterval = model.SamplingInterval,
                TriggerId = model.TriggerId,
                HeartbeatInterval = model.HeartbeatInterval,
                PublishedVariableDisplayName = model.PublishedVariableDisplayName,
                SubstituteValue = model.SubstituteValue?.Copy()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetWriterUpdateRequestApiModel ToApiModel(
            this DataSetWriterUpdateRequestModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterUpdateRequestApiModel {
                WriterGroupId = model.WriterGroupId,
                DataSetName = model.DataSetName,
                GenerationId = model.GenerationId,
                MessageSettings = model.MessageSettings.ToApiModel(),
                DataSetFieldContentMask = (DataSetFieldContentMask?)model.DataSetFieldContentMask,
                KeyFrameCount = model.KeyFrameCount,
                KeyFrameInterval = model.KeyFrameInterval,
                ExtensionFields = model.ExtensionFields
                    .ToDictionary(kv => kv.Key, kv => kv.Value),
                SubscriptionSettings = model.SubscriptionSettings.ToApiModel(),
                User = model.User.ToApiModel()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetWriterUpdateRequestModel ToServiceModel(
            this DataSetWriterUpdateRequestApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterUpdateRequestModel {
                WriterGroupId = model.WriterGroupId,
                DataSetName = model.DataSetName,
                GenerationId = model.GenerationId,
                MessageSettings = model.MessageSettings.ToServiceModel(),
                DataSetFieldContentMask = (OpcUa.Publisher.Models.DataSetFieldContentMask?)model.DataSetFieldContentMask,
                KeyFrameCount = model.KeyFrameCount,
                KeyFrameInterval = model.KeyFrameInterval,
                ExtensionFields = model.ExtensionFields
                    .ToDictionary(kv => kv.Key, kv => kv.Value),
                SubscriptionSettings = model.SubscriptionSettings.ToServiceModel(),
                User = model.User.ToServiceModel()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetWriterAddRequestApiModel ToApiModel(
            this DataSetWriterAddRequestModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterAddRequestApiModel {
                WriterGroupId = model.WriterGroupId,
                MessageSettings = model.MessageSettings.ToApiModel(),
                DataSetFieldContentMask = (DataSetFieldContentMask?)model.DataSetFieldContentMask,
                KeyFrameCount = model.KeyFrameCount,
                KeyFrameInterval = model.KeyFrameInterval,
                EndpointId = model.EndpointId,
                ExtensionFields = model.ExtensionFields
                    .ToDictionary(kv => kv.Key, kv => kv.Value),
                DataSetName = model.DataSetName,
                SubscriptionSettings = model.SubscriptionSettings.ToApiModel(),
                User = model.User.ToApiModel()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetWriterAddRequestModel ToServiceModel(
            this DataSetWriterAddRequestApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterAddRequestModel {
                WriterGroupId = model.WriterGroupId,
                MessageSettings = model.MessageSettings.ToServiceModel(),
                DataSetFieldContentMask = (OpcUa.Publisher.Models.DataSetFieldContentMask?)model.DataSetFieldContentMask,
                KeyFrameCount = model.KeyFrameCount,
                KeyFrameInterval = model.KeyFrameInterval,
                EndpointId = model.EndpointId,
                ExtensionFields = model.ExtensionFields
                    .ToDictionary(kv => kv.Key, kv => kv.Value),
                DataSetName = model.DataSetName,
                SubscriptionSettings = model.SubscriptionSettings.ToServiceModel(),
                User = model.User.ToServiceModel()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetWriterAddResponseApiModel ToApiModel(
            this DataSetWriterAddResultModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterAddResponseApiModel {
                GenerationId = model.GenerationId,
                DataSetWriterId = model.DataSetWriterId
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetWriterAddResultModel ToServiceModel(
            this DataSetWriterAddResponseApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterAddResultModel {
                GenerationId = model.GenerationId,
                DataSetWriterId = model.DataSetWriterId
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetWriterInfoApiModel ToApiModel(
            this DataSetWriterInfoModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterInfoApiModel {
                WriterGroupId = model.WriterGroupId,
                MessageSettings = model.MessageSettings.ToApiModel(),
                Updated = model.Updated.ToApiModel(),
                Created = model.Created.ToApiModel(),
                DataSet = model.DataSet.ToApiModel(),
                DataSetFieldContentMask = (DataSetFieldContentMask?)model.DataSetFieldContentMask,
                DataSetMetaDataSendInterval = model.DataSetMetaDataSendInterval,
                IsDisabled = model.IsDisabled,
                DataSetWriterId = model.DataSetWriterId,
                GenerationId = model.GenerationId,
                KeyFrameCount = model.KeyFrameCount,
                KeyFrameInterval = model.KeyFrameInterval,
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetWriterInfoModel ToServiceModel(
            this DataSetWriterInfoApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterInfoModel {
                WriterGroupId = model.WriterGroupId,
                MessageSettings = model.MessageSettings.ToServiceModel(),
                Updated = model.Updated.ToServiceModel(),
                Created = model.Created.ToServiceModel(),
                DataSet = model.DataSet.ToServiceModel(),
                DataSetFieldContentMask = (OpcUa.Publisher.Models.DataSetFieldContentMask?)model.DataSetFieldContentMask,
                DataSetMetaDataSendInterval = model.DataSetMetaDataSendInterval,
                IsDisabled = model.IsDisabled,
                DataSetWriterId = model.DataSetWriterId,
                GenerationId = model.GenerationId,
                KeyFrameCount = model.KeyFrameCount,
                KeyFrameInterval = model.KeyFrameInterval,
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetWriterInfoListApiModel ToApiModel(
            this DataSetWriterInfoListModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterInfoListApiModel {
                ContinuationToken = model.ContinuationToken,
                DataSetWriters = model.DataSetWriters?
                    .Select(w => w.ToApiModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetWriterInfoListModel ToServiceModel(
            this DataSetWriterInfoListApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterInfoListModel {
                ContinuationToken = model.ContinuationToken,
                DataSetWriters = model.DataSetWriters?
                    .Select(w => w.ToServiceModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static DataSetWriterInfoQueryApiModel ToApiModel(
            this DataSetWriterInfoQueryModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterInfoQueryApiModel {
                DataSetName = model.DataSetName,
                EndpointId = model.EndpointId,
                WriterGroupId = model.WriterGroupId
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static DataSetWriterInfoQueryModel ToServiceModel(
            this DataSetWriterInfoQueryApiModel model) {
            if (model == null) {
                return null;
            }
            return new DataSetWriterInfoQueryModel {
                DataSetName = model.DataSetName,
                EndpointId = model.EndpointId,
                WriterGroupId = model.WriterGroupId
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static PublishedDataSetSourceInfoApiModel ToApiModel(
            this PublishedDataSetSourceInfoModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetSourceInfoApiModel {
                Name = model.Name,
                OperationTimeout = model.OperationTimeout,
                State = model.State.ToApiModel(),
                SubscriptionSettings = model.SubscriptionSettings.ToApiModel(),
                User = model.User.ToApiModel(),
                DiagnosticsLevel = (Core.Models.DiagnosticsLevel?)model.DiagnosticsLevel,
                EndpointId = model.EndpointId,
                ExtensionFields = model.ExtensionFields?
                    .ToDictionary(kv => kv.Key, kv => kv.Value)
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static PublishedDataSetSourceInfoModel ToServiceModel(
            this PublishedDataSetSourceInfoApiModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetSourceInfoModel {
                Name = model.Name,
                OperationTimeout = model.OperationTimeout,
                State = model.State.ToServiceModel(),
                SubscriptionSettings = model.SubscriptionSettings.ToServiceModel(),
                User = model.User.ToServiceModel(),
                DiagnosticsLevel = (OpcUa.Core.Models.DiagnosticsLevel?)model.DiagnosticsLevel,
                EndpointId = model.EndpointId,
                ExtensionFields = model.ExtensionFields?
                    .ToDictionary(kv => kv.Key, kv => kv.Value)
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static PublishedDataSetSourceStateApiModel ToApiModel(
            this PublishedDataSetSourceStateModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetSourceStateApiModel {
                LastResult = model.LastResult.ToApiModel(),
                LastResultChange = model.LastResultChange
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static PublishedDataSetSourceStateModel ToServiceModel(
            this PublishedDataSetSourceStateApiModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetSourceStateModel {
                LastResult = model.LastResult.ToServiceModel(),
                LastResultChange = model.LastResultChange
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static PublishedDataSetVariableListApiModel ToApiModel(
            this PublishedDataSetVariableListModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetVariableListApiModel {
                ContinuationToken = model.ContinuationToken,
                Variables = model.Variables?
                    .Select(v => v.ToApiModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static PublishedDataSetVariableListModel ToServiceModel(
            this PublishedDataSetVariableListApiModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetVariableListModel {
                ContinuationToken = model.ContinuationToken,
                Variables = model.Variables?
                    .Select(v => v.ToServiceModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static PublishedDataSetVariableQueryApiModel ToApiModel(
            this PublishedDataSetVariableQueryModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetVariableQueryApiModel {
                Attribute = (Core.Models.NodeAttribute?)model.Attribute,
                PublishedVariableDisplayName = model.PublishedVariableDisplayName,
                PublishedVariableNodeId = model.PublishedVariableNodeId
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static PublishedDataSetVariableQueryModel ToServiceModel(
            this PublishedDataSetVariableQueryApiModel model) {
            if (model == null) {
                return null;
            }
            return new PublishedDataSetVariableQueryModel {
                Attribute = (OpcUa.Core.Models.NodeAttribute?)model.Attribute,
                PublishedVariableDisplayName = model.PublishedVariableDisplayName,
                PublishedVariableNodeId = model.PublishedVariableNodeId
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static WriterGroupAddRequestApiModel ToApiModel(
            this WriterGroupAddRequestModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupAddRequestApiModel {
                SiteId = model.SiteId,
                BatchSize = model.BatchSize,
                HeaderLayoutUri = model.HeaderLayoutUri,
                KeepAliveTime = model.KeepAliveTime,
                LocaleIds = model.LocaleIds?.ToList(),
                MessageSettings = model.MessageSettings.ToApiModel(),
                MessageType = (NetworkMessageType?)model.MessageType,
                Name = model.Name,
                Priority = model.Priority,
                PublishingInterval = model.PublishingInterval
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static WriterGroupAddRequestModel ToServiceModel(
            this WriterGroupAddRequestApiModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupAddRequestModel {
                SiteId = model.SiteId,
                BatchSize = model.BatchSize,
                HeaderLayoutUri = model.HeaderLayoutUri,
                KeepAliveTime = model.KeepAliveTime,
                LocaleIds = model.LocaleIds?.ToList(),
                MessageSettings = model.MessageSettings.ToServiceModel(),
                MessageType = (OpcUa.Publisher.Models.NetworkMessageType?)model.MessageType,
                Name = model.Name,
                Priority = model.Priority,
                PublishingInterval = model.PublishingInterval
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static WriterGroupAddResponseApiModel ToApiModel(
            this WriterGroupAddResultModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupAddResponseApiModel {
                GenerationId = model.GenerationId,
                WriterGroupId = model.WriterGroupId
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static WriterGroupAddResultModel ToServiceModel(
            this WriterGroupAddResponseApiModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupAddResultModel {
                GenerationId = model.GenerationId,
                WriterGroupId = model.WriterGroupId
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static WriterGroupStateApiModel ToApiModel(
            this WriterGroupStateModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupStateApiModel {
                State = (WriterGroupState)model.State,
                LastStateChange = model.LastStateChange
            };
        }

        /// <summary>
        /// Create service model from api model
        /// </summary>
        public static WriterGroupStateModel ToServiceModel(
            this WriterGroupStateApiModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupStateModel {
                State = (OpcUa.Publisher.WriterGroupState)model.State,
                LastStateChange = model.LastStateChange
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static WriterGroupInfoListApiModel ToApiModel(
            this WriterGroupInfoListModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupInfoListApiModel {
                ContinuationToken = model.ContinuationToken,
                WriterGroups = model.WriterGroups?
                    .Select(g => g.ToApiModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static WriterGroupInfoListModel ToServiceModel(
            this WriterGroupInfoListApiModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupInfoListModel {
                ContinuationToken = model.ContinuationToken,
                WriterGroups = model.WriterGroups?
                    .Select(g => g.ToServiceModel())
                    .ToList()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static WriterGroupInfoQueryApiModel ToApiModel(
            this WriterGroupInfoQueryModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupInfoQueryApiModel {
                Priority = model.Priority,
                Name = model.Name,
                MessageType = (NetworkMessageType?)model.MessageType,
                GroupVersion = model.GroupVersion,
                SiteId = model.SiteId
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static WriterGroupInfoQueryModel ToServiceModel(
            this WriterGroupInfoQueryApiModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupInfoQueryModel {
                Priority = model.Priority,
                Name = model.Name,
                MessageType = (OpcUa.Publisher.Models.NetworkMessageType?)model.MessageType,
                GroupVersion = model.GroupVersion,
                SiteId = model.SiteId
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static WriterGroupUpdateRequestApiModel ToApiModel(
            this WriterGroupUpdateRequestModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupUpdateRequestApiModel {
                BatchSize = model.BatchSize,
                GenerationId = model.GenerationId,
                HeaderLayoutUri = model.HeaderLayoutUri,
                KeepAliveTime = model.KeepAliveTime,
                LocaleIds = model.LocaleIds?.ToList(),
                MessageSettings = model.MessageSettings.ToApiModel(),
                MessageType = (NetworkMessageType?)model.MessageType,
                Name = model.Name,
                Priority = model.Priority,
                PublishingInterval = model.PublishingInterval
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static WriterGroupUpdateRequestModel ToServiceModel(
            this WriterGroupUpdateRequestApiModel model) {
            if (model == null) {
                return null;
            }
            return new WriterGroupUpdateRequestModel {
                BatchSize = model.BatchSize,
                GenerationId = model.GenerationId,
                HeaderLayoutUri = model.HeaderLayoutUri,
                KeepAliveTime = model.KeepAliveTime,
                LocaleIds = model.LocaleIds?.ToList(),
                MessageSettings = model.MessageSettings.ToServiceModel(),
                MessageType = (OpcUa.Publisher.Models.NetworkMessageType?)model.MessageType,
                Name = model.Name,
                Priority = model.Priority,
                PublishingInterval = model.PublishingInterval
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static PublisherOperationContextApiModel ToApiModel(
            this PublisherOperationContextModel model) {
            if (model == null) {
                return null;
            }
            return new PublisherOperationContextApiModel {
                AuthorityId = model.AuthorityId,
                Time = model.Time
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static PublisherOperationContextModel ToServiceModel(
            this PublisherOperationContextApiModel model) {
            if (model == null) {
                return null;
            }
            return new PublisherOperationContextModel {
                AuthorityId = model.AuthorityId,
                Time = model.Time
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <returns></returns>
        public static RequestHeaderApiModel ToApiModel(
            this RequestHeaderModel model) {
            if (model == null) {
                return null;
            }
            return new RequestHeaderApiModel {
                Diagnostics = model.Diagnostics?.ToApiModel(),
                Elevation = model.Elevation?.ToApiModel(),
                Locales = model.Locales?.ToList()
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <returns></returns>
        public static RequestHeaderModel ToServiceModel(
            this RequestHeaderApiModel model) {
            if (model == null) {
                return null;
            }
            return new RequestHeaderModel {
                Diagnostics = model.Diagnostics?.ToServiceModel(),
                Elevation = model.Elevation?.ToServiceModel(),
                Locales = model.Locales?.ToList()
            };
        }

        /// <summary>
        /// Create api model from service model
        /// </summary>
        /// <param name="model"></param>
        public static ServiceResultApiModel ToApiModel(
            this ServiceResultModel model) {
            if (model == null) {
                return null;
            }
            return new ServiceResultApiModel {
                Diagnostics = model.Diagnostics,
                ErrorMessage = model.ErrorMessage,
                StatusCode = model.StatusCode
            };
        }

        /// <summary>
        /// Convert back to service model
        /// </summary>
        /// <param name="model"></param>
        public static ServiceResultModel ToServiceModel(
            this ServiceResultApiModel model) {
            if (model == null) {
                return null;
            }
            return new ServiceResultModel {
                Diagnostics = model.Diagnostics,
                ErrorMessage = model.ErrorMessage,
                StatusCode = model.StatusCode
            };
        }

    }
}