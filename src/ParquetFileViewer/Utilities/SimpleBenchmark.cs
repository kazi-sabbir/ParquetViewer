using System;
using System.Diagnostics;

namespace ParquetFileViewer.Utilities
{
    public class SimpleBenchmark : IDisposable
    {
        private const string UnknownBenchmark = "Unknown Benchmark";
        private Stopwatch stopWatch;
        private string benchmarkName;

        public SimpleBenchmark(string benchmarkName)
        {
            this.stopWatch = new Stopwatch();
            this.stopWatch.Start();
            this.benchmarkName = !string.IsNullOrWhiteSpace(benchmarkName) ? benchmarkName : UnknownBenchmark;
        }

        public void Dispose()
        {
            this.stopWatch.Stop();
            Debug.WriteLine($"{this.benchmarkName}: Completed in {(int)this.stopWatch.Elapsed.TotalMinutes}:{this.stopWatch.Elapsed.Seconds:00}");
        }
    }
}
