// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Services.Tests {
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Services;
    using Microsoft.Azure.IIoT.OpcUa.Edge.Publisher.Models;
    using Microsoft.Azure.IIoT.Diagnostics;
    using Microsoft.Azure.IIoT.Serializers.NewtonSoft;
    using Microsoft.Azure.IIoT.Serializers;
    using Xunit;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System;

    /// <summary>
    /// Test
    /// </summary>
    public class PublishedNodesFileLoaderTests {

        [Fact]
        public void PnPlcEmptyTest() {
            var pn = @"
[
]
";
            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn));

            // No writers
            Assert.NotNull(group);
            Assert.Empty(group.DataSetWriters);
        }

        [Fact]
        public void PnPlcHeartbeatInterval2Test() {

            var pn = @"
[
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""OpcNodes"": [
            {
                ""Id"": ""i=2258"",
                ""HeartbeatInterval"": 2
            }
        ]
    }
]
";
            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn));

            Assert.NotEmpty(group.DataSetWriters);
            Assert.Single(group.DataSetWriters);
            Assert.Equal("opc.tcp://localhost:50000", group
                .DataSetWriters
                .Single().DataSet.DataSetSource.Connection.Endpoint.Url);
            Assert.Equal(2, group
                .DataSetWriters.Single()
                .DataSet.DataSetSource.PublishedVariables.PublishedData.Single()
                .HeartbeatInterval.Value.TotalSeconds);
        }

        [Fact]
        public void PnPlcHeartbeatSkipSingleTrueTest() {

            var pn = @"
[
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""OpcNodes"": [
            {
                ""Id"": ""i=2258"",
                ""SkipSingle"": true
            }
        ]
    }
]
";
            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn));

            Assert.NotNull(group);
            Assert.NotEmpty(group.DataSetWriters);
            Assert.Single(group.DataSetWriters);
            Assert.Single(group
                .DataSetWriters);
            Assert.Equal("opc.tcp://localhost:50000", group
                .DataSetWriters
                .Single().DataSet.DataSetSource.Connection.Endpoint.Url);
        }


        [Fact]
        public void PnPlcHeartbeatSkipSingleFalseTest() {

            var pn = @"
[
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""OpcNodes"": [
            {
                ""Id"": ""i=2258"",
                ""SkipSingle"": false
            }
        ]
    }
]
";
            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn));

            Assert.NotNull(group);
            Assert.NotEmpty(group.DataSetWriters);
            Assert.Single(group.DataSetWriters);
            Assert.Equal("opc.tcp://localhost:50000", group
                .DataSetWriters
                .Single().DataSet.DataSetSource.Connection.Endpoint.Url);
        }

        [Fact]
        public void PnPlcPublishingInterval2000Test() {

            var pn = @"
[
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""OpcNodes"": [
            {
                ""Id"": ""i=2258"",
                ""OpcPublishingInterval"": 2000
            }
        ]
    }
]
";
            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn));

            Assert.NotNull(group);
            Assert.NotEmpty(group.DataSetWriters);
            Assert.Single(group.DataSetWriters);
            Assert.Equal("opc.tcp://localhost:50000", group
                .DataSetWriters
                .Single().DataSet.DataSetSource.Connection.Endpoint.Url);
        }

        [Fact]
        public void PnPlcSamplingInterval2000Test() {

            var pn = @"
[
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""OpcNodes"": [
            {
                ""Id"": ""i=2258"",
                ""OpcSamplingInterval"": 2000
            }
        ]
    }
]
";
            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn));

            Assert.NotNull(group);
            Assert.NotEmpty(group.DataSetWriters);
            Assert.Single(group.DataSetWriters);
            Assert.Equal("opc.tcp://localhost:50000", group
                .DataSetWriters
                .Single().DataSet.DataSetSource.Connection.Endpoint.Url);
        }

        [Fact]
        public void PnPlcExpandedNodeIdTest() {

            var pn = @"
[
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""OpcNodes"": [
            {
                ""ExpandedNodeId"": ""nsu=http://opcfoundation.org/UA/;i=2258""
            }
        ]
    }
]
";
            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn));

            Assert.NotNull(group);
            Assert.NotEmpty(group.DataSetWriters);
            Assert.Single(group.DataSetWriters);
            Assert.Equal("opc.tcp://localhost:50000", group
                .DataSetWriters
                .Single().DataSet.DataSetSource.Connection.Endpoint.Url);
        }


        [Fact]
        public void PnPlcExpandedNodeId2Test() {

            var pn = @"
[
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""OpcNodes"": [
            {
                ""ExpandedNodeId"": ""nsu=http://opcfoundation.org/UA/;i=2258""
            }
        ]
    },
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""OpcNodes"": [
            {
                ""Id"": ""i=2262""
            },
            {
                ""Id"": ""ns=2;s=AlternatingBoolean""
            },
            {
                ""Id"": ""nsu=http://microsoft.com/Opc/OpcPlc/;s=NegativeTrendData""
            }
        ]
    }
]
";
            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn));

            Assert.NotNull(group);
            Assert.NotEmpty(group.DataSetWriters);
            Assert.Single(group.DataSetWriters);
            Assert.Single(group
                .DataSetWriters);
            Assert.Equal("opc.tcp://localhost:50000", group
                .DataSetWriters
                .Single().DataSet.DataSetSource.Connection.Endpoint.Url);
        }


        [Fact]
        public void PnPlcExpandedNodeId3Test() {

            var pn = @"
[
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""OpcNodes"": [
            {
                ""Id"": ""i=2258""
            },
            {
                ""Id"": ""ns=2;s=DipData""
            },
            {
                ""Id"": ""nsu=http://microsoft.com/Opc/OpcPlc/;s=NegativeTrendData""
            }
        ]
    }
]
";
            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn));

            Assert.NotNull(group);
            Assert.NotEmpty(group.DataSetWriters);
            Assert.Single(group.DataSetWriters);
            Assert.Single(group
              .DataSetWriters);
            Assert.Equal("opc.tcp://localhost:50000", group
                .DataSetWriters
                .Single().DataSet.DataSetSource.Connection.Endpoint.Url);
        }


        [Fact]
        public void PnPlcExpandedNodeId4Test() {

            var pn = @"
[
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""NodeId"": {
                ""Identifier"": ""i=2258""
        }
        },
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""NodeId"": {
            ""Identifier"": ""ns=0;i=2261""
        }
    },
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""OpcNodes"": [

            {
                ""ExpandedNodeId"": ""nsu=http://microsoft.com/Opc/OpcPlc/;s=AlternatingBoolean""
            }
        ]
    },
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""OpcNodes"": [
            {
                ""Id"": ""i=2262""
            },
            {
                ""Id"": ""ns=2;s=DipData""
            },
            {
                ""Id"": ""nsu=http://microsoft.com/Opc/OpcPlc/;s=NegativeTrendData""
            }
        ]
    }
]
";
            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn));

            Assert.NotNull(group);
            Assert.NotEmpty(group.DataSetWriters);
            Assert.Single(group.DataSetWriters);
            Assert.Single(group
                .DataSetWriters);
            Assert.Equal("opc.tcp://localhost:50000", group
                .DataSetWriters
                .Single().DataSet.DataSetSource.Connection.Endpoint.Url);
        }

        [Fact]
        public void PnPlcMultiJob1Test() {

            var pn = @"
[
    {
        ""EndpointUrl"": ""opc.tcp://localhost1:50000"",
        ""NodeId"": {
                ""Identifier"": ""i=2258""
        }
        },
    {
        ""EndpointUrl"": ""opc.tcp://localhost2:50000"",
        ""NodeId"": {
            ""Identifier"": ""ns=0;i=2261""
        }
    },
    {
        ""EndpointUrl"": ""opc.tcp://localhost3:50000"",
        ""OpcNodes"": [

            {
                ""ExpandedNodeId"": ""nsu=http://microsoft.com/Opc/OpcPlc/;s=AlternatingBoolean""
            }
        ]
    },
    {
        ""EndpointUrl"": ""opc.tcp://localhost4:50000"",
        ""OpcNodes"": [
            {
                ""Id"": ""i=2262""
            },
            {
                ""Id"": ""ns=2;s=DipData""
            },
            {
                ""Id"": ""nsu=http://microsoft.com/Opc/OpcPlc/;s=NegativeTrendData""
            }
        ]
    }
]
";
            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn));

            // No group
            Assert.NotNull(group);
            Assert.NotEmpty(group.DataSetWriters);
            Assert.Equal(4, group.DataSetWriters.Count());
        }

        [Fact]
        public void PnPlcMultiJob2Test() {

            var pn = @"
[
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50001"",
        ""NodeId"": {
                ""Identifier"": ""i=2258"",
        }
        },
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50002"",
        ""NodeId"": {
            ""Identifier"": ""ns=0;i=2261""
        }
    },
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50003"",
        ""OpcNodes"": [
            {
                ""OpcPublishingInterval"": 1000,
                ""ExpandedNodeId"": ""nsu=http://microsoft.com/Opc/OpcPlc/;s=AlternatingBoolean""
            }
        ]
    },
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50004"",
        ""OpcNodes"": [
            {
                ""OpcPublishingInterval"": 1000,
                ""Id"": ""i=2262""
            },
            {
                ""OpcPublishingInterval"": 1000,
                ""Id"": ""ns=2;s=DipData""
            },
            {
                ""Id"": ""nsu=http://microsoft.com/Opc/OpcPlc/;s=NegativeTrendData"",
                ""OpcPublishingInterval"": 1000
            }
        ]
    }
]
";
            var endpointUrls = new string [] {
                "opc.tcp://localhost:50001",
                "opc.tcp://localhost:50002",
                "opc.tcp://localhost:50003",
                "opc.tcp://localhost:50004"
            };

            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn));

            // No group
            Assert.NotNull(group);
            Assert.NotEmpty(group.DataSetWriters);
            Assert.Equal(4, group.DataSetWriters.Count());
            Assert.Equal(endpointUrls,
                group.DataSetWriters.Select(w => w.DataSet.DataSetSource.Connection.Endpoint.Url));
        }

        [Fact]
        public void PnPlcMultiJob3Test() {

            var pn = @"
[
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""NodeId"": {
                ""Identifier"": ""i=2258"",
        }
        },
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""NodeId"": {
            ""Identifier"": ""ns=0;i=2261""
        }
    },
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50001"",
        ""OpcNodes"": [
            {
                ""OpcPublishingInterval"": 1000,
                ""ExpandedNodeId"": ""nsu=http://microsoft.com/Opc/OpcPlc/;s=AlternatingBoolean""
            }
        ]
    },
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50001"",
        ""OpcNodes"": [
            {
                ""OpcPublishingInterval"": 1000,
                ""Id"": ""i=2262""
            },
            {
                ""OpcPublishingInterval"": 1000,
                ""Id"": ""ns=2;s=DipData""
            },
            {
                ""Id"": ""nsu=http://microsoft.com/Opc/OpcPlc/;s=NegativeTrendData"",
                ""OpcPublishingInterval"": 1000
            }
        ]
    }
]
";
            var endpointUrls = new string[] {
                "opc.tcp://localhost:50000",
                "opc.tcp://localhost:50001",
            };

            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn));

            // No group
            Assert.NotNull(group);
            Assert.NotEmpty(group.DataSetWriters);
            Assert.Equal(2, group.DataSetWriters.Count());
            Assert.Equal(endpointUrls,
                group.DataSetWriters.Select(w => w.DataSet.DataSetSource.Connection.Endpoint.Url));
        }

        [Fact]
        public void PnPlcMultiJob4Test() {

            var pn = @"
[
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""NodeId"": {
                ""Identifier"": ""i=2258"",
        }
        },
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""NodeId"": {
            ""Identifier"": ""ns=0;i=2261""
        }
    },
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50001"",
        ""OpcNodes"": [
            {
                ""ExpandedNodeId"": ""nsu=http://microsoft.com/Opc/OpcPlc/;s=AlternatingBoolean""
            }
        ]
    },
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50001"",
        ""OpcNodes"": [
            {
                ""OpcPublishingInterval"": 1000,
                ""Id"": ""i=2262""
            },
            {
                ""OpcPublishingInterval"": 1000,
                ""Id"": ""ns=2;s=DipData""
            },
            {
                ""Id"": ""nsu=http://microsoft.com/Opc/OpcPlc/;s=NegativeTrendData"",
            }
        ]
    }
]
";
            var endpointUrls = new string[] {
                "opc.tcp://localhost:50000",
                "opc.tcp://localhost:50001",
                "opc.tcp://localhost:50001",
            };

            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn));

            // No group
            Assert.NotNull(group);
            Assert.NotEmpty(group.DataSetWriters);
            Assert.Equal(3, group.DataSetWriters.Count());
            Assert.Equal(endpointUrls,
                group.DataSetWriters.Select(w => w.DataSet.DataSetSource.Connection.Endpoint.Url));
        }

        [Fact]
        public void PnPlcMultiJobBatching1Test() {

            var pn = new StringBuilder(@"
[
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""OpcNodes"": [
            ");

            for(var i = 1; i < 10000; i++) {
                pn.Append("{ \"Id\": \"i=");
                pn.Append(i);
                pn.Append("\" },");
            }

            pn.Append(@"
            { ""Id"": ""i=10000"" }
        ]
    }
]
");
            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn.ToString()));

            // No group
            Assert.NotNull(group);
            Assert.NotEmpty(group.DataSetWriters);
            Assert.Equal(10, group.DataSetWriters.Count());
            Assert.All(group.DataSetWriters, dataSetWriter => Assert.Equal("opc.tcp://localhost:50000",
                dataSetWriter.DataSet.DataSetSource.Connection.Endpoint.Url));
            Assert.All(group.DataSetWriters, dataSetWriter => Assert.Null(
                dataSetWriter.DataSet.DataSetSource.SubscriptionSettings.PublishingInterval));
            Assert.All(group.DataSetWriters, dataSetWriter => Assert.All(
                dataSetWriter.DataSet.DataSetSource.PublishedVariables.PublishedData,
                    p => Assert.Null(p.SamplingInterval)));
            Assert.All(group.DataSetWriters, dataSetWriter =>
                Assert.Equal(1000,
                    dataSetWriter.DataSet.DataSetSource.PublishedVariables.PublishedData.Count));
        }

        [Fact]
        public void PnPlcMultiJobBatching2Test() {

            var pn = new StringBuilder(@"
[
    {
        ""EndpointUrl"": ""opc.tcp://localhost:50000"",
        ""OpcNodes"": [
            ");

            for (var i = 1; i < 10000; i++) {
                pn.Append("{ \"Id\": \"i=");
                pn.Append(i);
                pn.Append("\"");
                pn.Append(i % 2 == 1 ? ",\"OpcPublishingInterval\": 1000" : null);
                pn.Append("},");
            }

            pn.Append(@"
            { ""Id"": ""i=10000"" }
        ]
    }
]
");
            var converter = new PublishedNodesFile(_serializer,
                new LegacyCliModel(), TraceLogger.Create());
            var group = converter.Read(new StringReader(pn.ToString()));

            // No group
            Assert.NotNull(group);
            Assert.NotEmpty(group.DataSetWriters);
            Assert.Equal(10, group.DataSetWriters.Count());
            Assert.All(group.DataSetWriters, dataSetWriter => Assert.Equal("opc.tcp://localhost:50000",
                dataSetWriter.DataSet.DataSetSource.Connection.Endpoint.Url));
            Assert.Equal(group.DataSetWriters.Select(dataSetWriter =>
                dataSetWriter.DataSet.DataSetSource.SubscriptionSettings?.PublishingInterval).ToList(),
                new TimeSpan?[] {
                    TimeSpan.FromMilliseconds(1000),
                    TimeSpan.FromMilliseconds(1000),
                    TimeSpan.FromMilliseconds(1000),
                    TimeSpan.FromMilliseconds(1000),
                    TimeSpan.FromMilliseconds(1000),
                    null, null, null, null, null});

            Assert.All(group.DataSetWriters, dataSetWriter => Assert.All(
                dataSetWriter.DataSet.DataSetSource.PublishedVariables.PublishedData,
                    p => Assert.Null(p.SamplingInterval)));
            Assert.All(group.DataSetWriters, dataSetWriter =>
                Assert.Equal(1000,
                    dataSetWriter.DataSet.DataSetSource.PublishedVariables.PublishedData.Count));
        }

        private readonly IJsonSerializer _serializer = new NewtonSoftJsonSerializer();
    }
}
