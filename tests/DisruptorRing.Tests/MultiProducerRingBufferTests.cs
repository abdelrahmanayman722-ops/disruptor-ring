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

    [Fact]
    public void MultiProducer_ClaimWithoutPublish_IsEmptyRemainsTrue()
    {
        var ring = Disruptor.CreateMultiProducer<int>(16);
        Assert.True(ring.IsEmpty);
        Assert.Equal(0, ring.AvailableCount);

        long seq = ring.Claim();
        // Slot is claimed but not published yet
        Assert.True(ring.IsEmpty);
        Assert.Equal(0, ring.AvailableCount);
        Assert.False(ring.TryConsume(out _));

        ref int slot = ref ring.Get(seq);
        slot = 777;
        ring.Commit(seq);

        // Now it is published and consumable
        Assert.False(ring.IsEmpty);
        Assert.Equal(1, ring.AvailableCount);
        Assert.True(ring.TryConsume(out int val));
        Assert.Equal(777, val);
        Assert.True(ring.IsEmpty);
    }

    [Fact]
    public void MultiProducer_BatchClaimAndCommit_MaintainsOrder()
    {
        var ring = Disruptor.CreateMultiProducer<int>(64);
        int batchSize = 10;

        long hi = ring.Claim(batchSize);
        long lo = hi - batchSize + 1;

        for (long s = lo; s <= hi; s++)
        {
            ring.Get(s) = (int)(s * 10);
        }

        ring.Commit(lo, hi);

        Assert.Equal(batchSize, ring.AvailableCount);

        for (long s = lo; s <= hi; s++)
        {
            Assert.True(ring.TryConsume(out int val));
            Assert.Equal((int)(s * 10), val);
        }

        Assert.True(ring.IsEmpty);
    }

    [Fact]
    public async Task MultiProducer_YieldingWaitStrategy_ConsumesSuccessfully()
    {
        var sequencer = new MultiProducerSequencer(64, new YieldingWaitStrategy());
        var ring = new RingBuffer<int>(sequencer);

        var consumer = Task.Run(() =>
        {
            int sum = 0;
            for (int i = 0; i < 50; i++)
            {
                sum += ring.Consume();
            }
            return sum;
        });

        for (int i = 1; i <= 50; i++)
        {
            ring.Publish(i);
        }

        int result = await consumer;
        Assert.Equal(1275, result);
    }
}
