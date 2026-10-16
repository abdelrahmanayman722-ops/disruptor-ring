namespace DisruptorRing.Tests;

using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

public class MultiProducerRingBufferTests
{
    [Fact]
    public async Task MultiProducer_ConcurrentPublishing_NoLossOrDuplication()
    {
        int capacity = 1024;
        int producerCount = 4;
        int itemsPerProducer = 10_000;
        int totalItems = producerCount * itemsPerProducer;

        var ring = Disruptor.CreateMultiProducer<int>(capacity);
        var received = new ConcurrentDictionary<int, int>();

        var consumerTask = Task.Run(() =>
        {
            int consumedCount = 0;
            while (consumedCount < totalItems)
            {
                if (ring.TryConsume(out int val))
                {
                    received.TryAdd(val, 1);
                    consumedCount++;
                }
                else
                {
                    Thread.SpinWait(1);
                }
            }
        });

        await Task.Run(() =>
        {
            Parallel.For(0, producerCount, p =>
            {
                int start = p * itemsPerProducer;
                for (int i = 0; i < itemsPerProducer; i++)
                {
                    ring.Publish(start + i);
                }
            });
        });

        await consumerTask;
        Assert.Equal(totalItems, received.Count);
    }
}
