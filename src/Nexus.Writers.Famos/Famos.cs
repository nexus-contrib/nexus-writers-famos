using ImcFamosFile;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using System.Text.Json;

namespace Nexus.Writers
{
    [DataWriterDescription(DESCRIPTION)]
    [ExtensionDescription(
        "Writes data into Famos files.",
        "https://github.com/Apollo3zehn/nexus-sources-famos",
        "https://github.com/Apollo3zehn/nexus-sources-famos")]
    public class Famos : IDataWriter
    {
        #region Fields

private const string DESCRIPTION = @"
{
  ""label"": ""imc FAMOS v2 (*.dat)""
}
        ";

        private FamosFile _famosFile = default!;
        private TimeSpan _lastSamplePeriod;
        private JsonSerializerOptions _serializerOptions;

        #endregion

        #region Properties

        private DataWriterContext Context { get; set; } = default!;

        #endregion

        #region Constructors

        public Famos()
        {
            _serializerOptions = new JsonSerializerOptions()
            {
                WriteIndented = true
            };
        }

        #endregion

        #region Methods

        public Task SetContextAsync(
            DataWriterContext context,
            ILogger logger,
            CancellationToken cancellationToken)
        {
            Context = context;
            return Task.CompletedTask;
        }

        public Task OpenAsync(
            DateTime fileBegin, 
            TimeSpan filePeriod,
            TimeSpan samplePeriod, 
            CatalogItem[] catalogItems, 
            CancellationToken cancellationToken)
        {
            return Task.Run(() =>
            {
                _lastSamplePeriod = samplePeriod;

                var totalLength = filePeriod.Ticks / samplePeriod.Ticks;
                var dx = samplePeriod.TotalSeconds;
                var root = Context.ResourceLocator.ToPath();
                var filePath = Path.Combine(root, $"{fileBegin.ToString("yyyy-MM-ddTHH-mm-ss")}Z_{samplePeriod.ToUnitString()}.dat");

                if (File.Exists(filePath))
                    throw new Exception($"The file {filePath} already exists. Extending an already existing file with additional resources is not supported.");

                var famosFile = new FamosFileHeader();

                // file
                var metadataGroup = new FamosFileGroup("Metadata");

                metadataGroup.PropertyInfo = new FamosFilePropertyInfo(new List<FamosFileProperty>()
                {
                    new FamosFileProperty("system_name", "Nexus"),
                    new FamosFileProperty("date_time", fileBegin.ToString("yyyy-MM-ddTHH-mm-ss") + "Z"),
                    new FamosFileProperty("sample_period", samplePeriod.ToUnitString()),
                });

                famosFile.Groups.Add(metadataGroup);

                var field = new FamosFileField(FamosFileFieldType.MultipleYToSingleEquidistantTime);

                foreach (var catalogItemGroup in catalogItems.GroupBy(catalogItem => catalogItem.Catalog))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    // file -> catalog
                    var catalog = catalogItemGroup.Key;
                    var catalogGroup = new FamosFileGroup(catalog.Id);

                    catalogGroup.PropertyInfo = new FamosFilePropertyInfo();

                    if (catalog.Properties is not null)
                    {
                        var key = "properties";
                        var value = JsonSerializer.Serialize(catalog.Properties, _serializerOptions);
                        catalogGroup.PropertyInfo.Properties.Add(new FamosFileProperty(key, value));
                    }

                    famosFile.Groups.Add(catalogGroup);

                    if (totalLength * (double)Famos.SizeOf(NexusDataType.FLOAT64) > 2 * Math.Pow(10, 9))
                        throw new Exception(ErrorMessage.FamosWriter_DataSizeExceedsLimit);

                    // file -> catalog -> resources
                    foreach (var catalogItem in catalogItemGroup)
                    {
                        var channel = PrepareChannel(field, catalogItem, (int)totalLength, fileBegin, dx);
                        catalogGroup.Channels.Add(channel);
                    }
                }

                famosFile.Fields.Add(field);

                //
                famosFile.Save(filePath, _ => { });
                _famosFile = FamosFile.OpenEditable(filePath);
            });
        }

        public Task WriteAsync(
            TimeSpan fileOffset,
            WriteRequest[] requests,
            IProgress<double> progress,
            CancellationToken cancellationToken)
        {
            return Task.Run(() =>
            {
                var offset = fileOffset.Ticks / _lastSamplePeriod.Ticks;

                var requestGroups = requests
                    .GroupBy(request => request.CatalogItem.Catalog)
                    .ToList();

                var processed = 0;
                var field = _famosFile.Fields.First();

                _famosFile.Edit(writer =>
                {
                    foreach (var requestGroup in requestGroups)
                    {
                        var catalog = requestGroup.Key;
                        var writeRequests = requestGroup.ToArray();

                        for (int i = 0; i < writeRequests.Length; i++)
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            var component = field.Components[i];
                            var data = writeRequests[i].Data;

                            _famosFile.WriteSingle(writer, component, (int)offset, data.Span);
                        }

                        processed++;
                        progress.Report((double)processed / requests.Length);
                    }
                });
            });
        }

        public Task CloseAsync(
            CancellationToken cancellationToken)
        {
            _famosFile.Dispose();
            return Task.CompletedTask;
        }

        private FamosFileChannel PrepareChannel(FamosFileField field, CatalogItem catalogItem, int totalLength, DateTime startDateTme, double dx)
        {
            // component 
            var representationName = $"{catalogItem.Resource.Id}_{catalogItem.Representation.Id}{GetRepresentationParameterString(catalogItem.Parameters)}";

            var unit = string.Empty;

            if (catalogItem.Resource.Properties is not null && 
                catalogItem.Resource.Properties.TryGetValue("unit", out var unitElement) &&
                unitElement.ValueKind == JsonValueKind.String)
            {
                unit = unitElement.GetString() ?? string.Empty;
            }

            var calibration = new FamosFileCalibration(false, 1, 0, false, unit);

            var component = new FamosFileAnalogComponent(representationName, FamosFileDataType.Float64, totalLength, calibration)
            {
                XAxisScaling = new FamosFileXAxisScaling((decimal)dx) { Unit = "s" },
                TriggerTime = new FamosFileTriggerTime(startDateTme, FamosFileTimeMode.Unknown),
            };

            // attributes
            var channel = component.Channels.First();

            channel.PropertyInfo = new FamosFilePropertyInfo();

            var properties = catalogItem.Resource.Properties;

            if (properties is not null)
            {
                var key = "properties";
                var value = JsonSerializer.Serialize(properties, _serializerOptions);
                channel.PropertyInfo.Properties.Add(new FamosFileProperty(key, value));
            }

            field.Components.Add(component);

            return channel;
        }

        private static string? GetRepresentationParameterString(IReadOnlyDictionary<string, string>? parameters)
        {
            if (parameters is null)
                return default;
            
            var serializedParameters = parameters
                .Select(parameter => $"{parameter.Key}={parameter.Value}");

            var parametersString = $"({string.Join(',', serializedParameters)})";

            return parametersString;
        }

        private static int SizeOf(NexusDataType dataType)
        {
            return ((ushort)dataType & 0x00FF) / 8;
        }

        #endregion
    }
}
