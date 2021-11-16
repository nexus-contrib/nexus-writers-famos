using ImcFamosFile;
using Microsoft.Extensions.Logging.Abstractions;
using Nexus.DataModel;
using Nexus.Extensibility;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Nexus.Writers.Famos.Tests
{
    public class FamosTests : IClassFixture<DataWriterFixture>
    {
        private DataWriterFixture _fixture;

        public FamosTests(DataWriterFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task CanWriteFiles()
        {
            var targetFolder = _fixture.GetTargetFolder();
            var dataWriter = new Famos() as IDataWriter;

            var context = new DataWriterContext(
                ResourceLocator: new Uri(targetFolder),
                Configuration: new Dictionary<string, string>(),
                Logger: NullLogger.Instance);

            await dataWriter.SetContextAsync(context, CancellationToken.None);

            var begin = new DateTime(2020, 01, 01, 0, 0, 0, DateTimeKind.Utc);
            var samplePeriod = TimeSpan.FromSeconds(1);

            var catalogItems = _fixture.Catalogs.SelectMany(catalog => catalog.Resources
                .SelectMany(resource => resource.Representations.Select(representation => new CatalogItem(catalog, resource, representation))))
                .ToArray();

            var random = new Random(Seed: 1);

            var length = 1000;

            var data = new[]
            {
                Enumerable
                    .Range(0, length)
                    .Select(value => random.NextDouble() * 1e4)
                    .ToArray(),

                Enumerable
                    .Range(0, length)
                    .Select(value => random.NextDouble() * -1)
                    .ToArray(),

                Enumerable
                    .Range(0, length)
                    .Select(value => random.NextDouble() * Math.PI)
                    .ToArray()
            };

            var requests = catalogItems
                .Select((catalogItem, i) => new WriteRequest(catalogItem, data[i]))
                .ToArray();

            await dataWriter.OpenAsync(begin, TimeSpan.FromSeconds(2000), samplePeriod, catalogItems, CancellationToken.None);
            await dataWriter.WriteAsync(TimeSpan.Zero, requests, new Progress<double>(), CancellationToken.None);
            await dataWriter.WriteAsync(TimeSpan.FromSeconds(length), requests, new Progress<double>(), CancellationToken.None);
            await dataWriter.CloseAsync(CancellationToken.None);

            var actualFilePaths = Directory
                .GetFiles(targetFolder)
                .OrderBy(value => value)
                .ToArray();

            // assert
            Assert.True(actualFilePaths.Length == 1);

            using var famosFile = FamosFile.Open(actualFilePaths.First());

            Assert.True(famosFile.Fields.Count == 1);

            var metadata = famosFile.Groups[0];
            Assert.Equal("Metadata", metadata.Name);

            // catalog 1
            var catalog1 = famosFile.Groups[1];
            Assert.Equal(_fixture.Catalogs[0].Id, catalog1.Name);
            var representations1 = _fixture.Catalogs[0].Resources.SelectMany(resource => resource.Representations).ToList();
            Assert.Equal(representations1.Count, catalog1.Channels.Count);
            var properties1 = catalog1.PropertyInfo.Properties;
            AssertProperties(_fixture.Catalogs[0].Properties, catalog1.PropertyInfo.Properties);
            AssertProperties(_fixture.Catalogs[0].Resources[0].Properties, catalog1.Channels[0].PropertyInfo.Properties);
            AssertProperties(_fixture.Catalogs[0].Resources[0].Properties, catalog1.Channels[1].PropertyInfo.Properties);

            // catalog 2
            var catalog2 = famosFile.Groups[2];
            Assert.Equal(_fixture.Catalogs[1].Id, catalog2.Name);
            var representations2 = _fixture.Catalogs[1].Resources.SelectMany(resource => resource.Representations).ToList();
            Assert.Equal(representations2.Count, catalog2.Channels.Count);
            AssertProperties(_fixture.Catalogs[1].Properties, catalog2.PropertyInfo.Properties);
            AssertProperties(_fixture.Catalogs[1].Resources[0].Properties, catalog2.Channels[0].PropertyInfo.Properties);

            void AssertProperties(IReadOnlyDictionary<string, string> expected, List<FamosFileProperty> actual)
            {
                Assert.Equal(expected.Count, actual.Count);

                for (int i = 0; i < actual.Count; i++)
                {
                    var key = actual[i].Name;
                    var value = actual[i].Value;

                    var match = expected[key];
                    Assert.Equal(match, value);
                }
            }
        }
    }
}