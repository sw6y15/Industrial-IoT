// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Publisher {
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Services;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Default;
    using Microsoft.Azure.IIoT.OpcUa.Publisher.Storage.Default;
    using Autofac;

    /// <summary>
    /// Injected publisher services
    /// </summary>
    public sealed class PublisherServices : Module {

        /// <summary>
        /// Load the module
        /// </summary>
        /// <param name="builder"></param>
        protected override void Load(ContainerBuilder builder) {

            builder.RegisterType<WriterGroupRegistry>()
                .AsImplementedInterfaces().InstancePerLifetimeScope();
            builder.RegisterType<WriterGroupManagement>()
                .AsImplementedInterfaces().InstancePerLifetimeScope();
            builder.RegisterType<DataSetWriterStateSync>()
                .AsImplementedInterfaces();

            builder.RegisterType<DataSetWriterEventBroker>()
                .AsImplementedInterfaces().InstancePerLifetimeScope();
            builder.RegisterType<WriterGroupEventBroker>()
                .AsImplementedInterfaces().InstancePerLifetimeScope();
            builder.RegisterType<PublishedDataSetEventBroker>()
                .AsImplementedInterfaces().InstancePerLifetimeScope();

            builder.RegisterType<WriterGroupDatabase>()
                .AsImplementedInterfaces().InstancePerLifetimeScope();
            builder.RegisterType<DataSetWriterDatabase>()
                .AsImplementedInterfaces().InstancePerLifetimeScope();
            builder.RegisterType<DataSetEntityDatabase>()
                .AsImplementedInterfaces().InstancePerLifetimeScope();

            base.Load(builder);
        }
    }
}
