namespace DisruptorRing.Benchmarks;

using System;
using System.Diagnostics;
using BenchmarkDotNet.Running;
using DisruptorRing;

public class Program
{
    public static void Main(string[] args)
    {
        if (args.Length > 0 && args[0].Equals("--benchmark", StringComparison.OrdinalIgnoreCase))
        {
            BenchmarkRunner.Run<RingBufferBenchmarks>();
            return;
        }

        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine("================================================================================");
        Console.WriteLine("  DISRUPTOR-RING // HIGH-PERFORMANCE ZERO-ALLOCATION LOCK-FREE PIPELINE");
        Console.WriteLine("================================================================================");
        Console.ResetColor();

        const int capacity = 65536;
        const int iterations = 10_000_000;

        Console.WriteLine($"[1/2] Initializing Single-Producer Disruptor Ring (Capacity: {capacity:N0})...");
        var ring = Disruptor.CreateSingleProducer<long>(capacity);

        // Warm up JIT
        for (int i = 0; i < 100_000; i++)
        {
            ring.Publish(i);
            ring.TryConsume(out _);
        }

        long beforeAllocated = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();

        for (int i = 0; i < iterations; i++)
        {
            ring.Publish(i);
            ring.TryConsume(out _);
        }

        sw.Stop();
        long afterAllocated = GC.GetAllocatedBytesForCurrentThread();

        double opsPerSec = (double)iterations / sw.Elapsed.TotalSeconds;
        double nsPerOp = (sw.Elapsed.TotalNanoseconds) / iterations;

        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"[2/2] Completed {iterations:N0} iterations in {sw.ElapsedMilliseconds} ms");
        Console.WriteLine($"      Throughput:  {opsPerSec:N0} ops/sec");
        Console.WriteLine($"      Latency:     {nsPerOp:F2} ns / op");
        Console.WriteLine($"      Heap Alloc:  {afterAllocated - beforeAllocated} bytes (Zero GC)");
        Console.ResetColor();

        Console.WriteLine();
        Console.WriteLine("To run the full BenchmarkDotNet suite, pass '--benchmark'.");
    }
}
