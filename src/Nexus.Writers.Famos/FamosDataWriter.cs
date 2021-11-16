using ImcFamosFile;
using Nexus.DataModel;
using Nexus.Extensibility;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Nexus.Writers.Famos
{
    [DataWriterFormatName("imc FAMOS v2 (*.dat)")]
    [ExtensionDescription("Writes data into Famos files.")]
    public class FamosDataWriter : IDataWriter
    {
        #region "Fields"

        private FamosFile _famosFile = null!;
        private TimeSpan _lastSamplePeriod;

        #endregion

        #region Properties

        private DataWriterContext Context { get; set; } = null!;

        #endregion

        #region "Methods"

        public Task SetContextAsync(
            DataWriterContext context,
            CancellationToken cancellationToken)
        {
            this.Context = context;
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
                var root = this.Context.ResourceLocator.ToPath();
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

                    var catalog = catalogItemGroup.Key;

                    // file -> catalog
                    var catalogGroup = new FamosFileGroup(catalog.Id);

                    catalogGroup.PropertyInfo = new FamosFilePropertyInfo();

                    if (catalog.Properties is not null)
                    {
                        foreach (var entry in catalog.Properties)
                        {
                            catalogGroup.PropertyInfo.Properties.Add(new FamosFileProperty(entry.Key, entry.Value));
                        }
                    }

                    famosFile.Groups.Add(catalogGroup);

                    // for each context group
                    if (totalLength * (double)FamosDataWriter.SizeOf(NexusDataType.FLOAT64) > 2 * Math.Pow(10, 9))
                        throw new Exception(ErrorMessage.FamosWriter_DataSizeExceedsLimit);

                    // file -> catalog -> resources
                    foreach (var catalogItem in catalogItemGroup)
                    {
                        var channel = this.PrepareChannel(field, catalogItem, (int)totalLength, fileBegin, dx);
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

                var field = _famosFile.Fields.First();

                _famosFile.Edit(writer =>
                {
                    foreach (var requestGroup in requestGroups)
                    {
                        var catalog = requestGroup.Key;
                        var requestGroupArray = requestGroup.ToArray();

                        for (int i = 0; i < requestGroupArray.Length; i++)
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            var component = field.Components[i];
                            var data = requestGroupArray[i].Data;

                            _famosFile.WriteSingle<double>(writer, component, (int)offset, data.Span);
                        }
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
            var representationName = $"{catalogItem.Resource.Id}_{catalogItem.Representation.Id}";

            var unit = catalogItem.Resource.Properties is null
                ? string.Empty
                : catalogItem.Resource.Properties.GetValueOrDefault("Unit", string.Empty);

            var calibration = new FamosFileCalibration(false, 1, 0, false, unit);

            var component = new FamosFileAnalogComponent(representationName, FamosFileDataType.Float64, totalLength, calibration)
            {
                XAxisScaling = new FamosFileXAxisScaling((decimal)dx) { Unit = "s" },
                TriggerTime = new FamosFileTriggerTime(startDateTme, FamosFileTimeMode.Unknown),
            };

            // attributes
            var channel = component.Channels.First();

            channel.PropertyInfo = new FamosFilePropertyInfo();

            if (catalogItem.Resource.Properties is not null)
            {
                foreach (var entry in catalogItem.Resource.Properties)
                {
                    channel.PropertyInfo.Properties.Add(new FamosFileProperty(entry.Key, entry.Value));
                }
            }

            field.Components.Add(component);

            return channel;
        }

        private static int SizeOf(NexusDataType dataType)
        {
            return ((ushort)dataType & 0x00FF) / 8;
        }

        #endregion
    }
}
