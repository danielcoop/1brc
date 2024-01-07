using System.Diagnostics;

namespace __1brc
{
    class Program
    {
        private static void Main(string[] args)
        {
            var path = args.Length > 0 ? args[0] : "E:\\temp\\measurements.txt";

            var sw = new Stopwatch();
            sw.Start();

            _ = new App(path);

            sw.Stop();
            Console.WriteLine($"Completed in: { sw.Elapsed }");
        }
    }
}