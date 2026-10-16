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
}
