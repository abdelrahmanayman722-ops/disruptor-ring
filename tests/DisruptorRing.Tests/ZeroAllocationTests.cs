namespace DisruptorRing.Tests;

using System;
using Xunit;

public class ZeroAllocationTests
{
    [Fact]
    public void HotPath_PublishAndConsume_ZeroGCAllocations()
    {
        var ring = Disruptor.CreateSingleProducer<long>(1024);

        // Warm up JIT and ensure all types and methods are compiled
        for (int i = 0; i < 1000; i++)
        {
            ring.Publish(i);
            ring.TryConsume(out _);
        }

        // Force full GC and measure baseline
        GC.Collect(2, GCCollectionMode.Forced, true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, true);

        long beforeAllocated = GC.GetAllocatedBytesForCurrentThread();

        const int iterations = 100_000;
        for (int i = 0; i < iterations; i++)
        {
            ring.Publish(i);
            ring.TryConsume(out _);
        }

        long afterAllocated = GC.GetAllocatedBytesForCurrentThread();
        long allocatedDelta = afterAllocated - beforeAllocated;

        // Hot loop MUST produce 0 bytes of heap allocation
        Assert.Equal(0, allocatedDelta);
    }
}
