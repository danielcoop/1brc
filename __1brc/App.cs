using System.Collections.Concurrent;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;

namespace __1brc
{
    internal class App
    {
        public App(string filePath)
        {
            var results = ProcessFile(filePath);

            foreach (var station in results.OrderBy(kv => kv.Key))
            {
                Console.WriteLine(station.Key);

                var (min, mean, max, count) = CalculateStatistics(station.Value);

                Console.WriteLine($"Min: {min}, Avg: {mean} Max: {max}");
            }
        }

        private static ConcurrentDictionary<string, (float min, float mean, float max, int count)> ProcessFile(string filePath)
        {
            var results = new ConcurrentDictionary<string, (float min, float mean, float max, int count)>();

            // Arbritrary 50MB chunk size...
            var chunkSize = Math.Max(1, 1024 * 1024 * 50);

            using (var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, chunkSize, true))
            using (var memoryMappedFile = MemoryMappedFile.CreateFromFile(fileStream, null, 0, MemoryMappedFileAccess.Read, HandleInheritability.None, false))
            using (var memoryMappedView = memoryMappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read))
            {
                var bufferSize = (int)memoryMappedView.Capacity;
                var buffer = new byte[bufferSize];

                memoryMappedView.ReadArray(0, buffer, 0, bufferSize);

                Parallel.ForEach(Partitioner.Create(0, bufferSize, chunkSize), new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, range =>
                {
                    for (int i = range.Item1; i < range.Item2;)
                    {
                        int lineLength = 0;
                        while (i + lineLength < bufferSize && buffer[i + lineLength] != (byte)'\n' && buffer[i + lineLength] != 0)
                        {
                            lineLength++;
                        }

                        if (lineLength > 0)
                        {
                            var stationInfo = ReadStationInfo(buffer, i, lineLength);
                            if (float.TryParse(stationInfo.temperature, out float temperature))
                            {
                                results.AddOrUpdate(
                                    stationInfo.station,
                                    _ => (temperature, temperature, temperature, 1),
                                    (_, existing) => (
                                        Math.Min(existing.min, temperature),
                                        existing.mean + (temperature - existing.mean) / (existing.count + 1),
                                        Math.Max(existing.max, temperature),
                                        existing.count + 1
                                    ));
                            }

                            i += lineLength + 1;
                        }
                        else
                        {
                            i++;
                        }
                    }
                });
            }

            return results;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe (string station, string temperature) ReadStationInfo(byte[] buffer, int startIndex, int length)
        {
            fixed (byte* pBuffer = buffer)
            {
                byte* pLine = pBuffer + startIndex;
                return ExtractStationInfo(pLine, length);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe (string station, string temperature) ExtractStationInfo(byte* pLine, int length)
        {
            var separatorIndex = -1;
            for (var i = 0; i < length; i++)
            {
                if (pLine[i] == (byte)';')
                {
                    separatorIndex = i;
                    break;
                }
            }

            if (separatorIndex != -1)
            {
                return (
                    new string((sbyte*)pLine, 0, separatorIndex, System.Text.Encoding.UTF8),
                    new string((sbyte*)pLine + separatorIndex + 1, 0, length - separatorIndex - 1, System.Text.Encoding.UTF8)
                );
            }

            return (string.Empty, string.Empty);
        }

        // TODO Do I need to round all values?
        private static (float min, float mean, float max, int count) CalculateStatistics((float min, float mean, float max, int count) values)
        {
            return (values.min, MathF.Round(values.mean, 1), values.max, values.count);
        }
    }
}
