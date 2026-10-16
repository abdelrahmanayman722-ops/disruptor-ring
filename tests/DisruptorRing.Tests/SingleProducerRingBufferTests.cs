namespace DisruptorRing.Tests;

using System.Threading.Tasks;
using Xunit;

public class SingleProducerRingBufferTests
{
    [Fact]
    public void PublishAndConsume_SingleItem_Succeeds()
    {
        var ring = Disruptor.CreateSingleProducer<int>(64);
        Assert.True(ring.IsEmpty);

        ring.Publish(42);
        Assert.False(ring.IsEmpty);
        Assert.Equal(1, ring.AvailableCount);

        bool consumed = ring.TryConsume(out int val);
        Assert.True(consumed);
        Assert.Equal(42, val);
        Assert.True(ring.IsEmpty);
    }

    [Fact]
    public void ClaimAndCommit_ZeroCopy_Succeeds()
    {
        var ring = Disruptor.CreateSingleProducer<long>(64);
        long seq = ring.Claim();
        ref long slot = ref ring.Get(seq);
        slot = 9999L;
        ring.Commit(seq);

        Assert.True(ring.TryConsume(out long item));
        Assert.Equal(9999L, item);
    }

    [Fact]
    public void FullBuffer_TryPublish_ReturnsFalse()
    {
        int capacity = 16;
        var ring = Disruptor.CreateSingleProducer<int>(capacity);

        for (int i = 0; i < capacity; i++)
        {
            Assert.True(ring.TryPublish(i));
        }

        // 17th item should fail without space
        Assert.False(ring.TryPublish(999));

        // Consume one item
        Assert.True(ring.TryConsume(out int first));
        Assert.Equal(0, first);

        // Now one slot should be available
        Assert.True(ring.TryPublish(999));
    }

    [Fact]
    public async Task WrapAround_HighThroughput_MaintainsStrictFIFO()
    {
        int capacity = 32;
        int count = 100_000;
        var ring = Disruptor.CreateSingleProducer<int>(capacity);

        var consumerTask = Task.Run(() =>
        {
            for (int i = 0; i < count; i++)
            {
                int item = ring.Consume();
                Assert.Equal(i, item);
            }
        });

        for (int i = 0; i < count; i++)
        {
            ring.Publish(i);
        }

        await consumerTask;
    }

    [Fact]
    public void BatchClaimAndCommit_ZeroCopy_Succeeds()
    {
        var ring = Disruptor.CreateSingleProducer<int>(64);
        int batchSize = 8;
        long hi = ring.Claim(batchSize);
        long lo = hi - batchSize + 1;

        for (long s = lo; s <= hi; s++)
        {
            ring.Get(s) = (int)(s * 5);
        }
        ring.Commit(lo, hi);

        Assert.Equal(batchSize, ring.AvailableCount);

        for (long s = lo; s <= hi; s++)
        {
            Assert.True(ring.TryConsume(out int val));
            Assert.Equal((int)(s * 5), val);
        }
        Assert.True(ring.IsEmpty);
    }

    [Fact]
    public async Task SingleProducer_YieldingWaitStrategy_ConsumesSuccessfully()
    {
        var sequencer = new SingleProducerSequencer(64, new YieldingWaitStrategy());
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
