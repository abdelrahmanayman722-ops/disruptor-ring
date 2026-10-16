namespace DisruptorRing.Tests;

using System;
using Xunit;

public class CapacityAndMaskTests
{
    [Theory]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(16)]
    [InlineData(64)]
    [InlineData(1024)]
    [InlineData(65536)]
    public void ValidPowerOfTwo_InstantiatesCorrectly(int capacity)
    {
        var singleRing = Disruptor.CreateSingleProducer<int>(capacity);
        Assert.Equal(capacity, singleRing.Capacity);

        var multiRing = Disruptor.CreateMultiProducer<long>(capacity);
        Assert.Equal(capacity, multiRing.Capacity);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    public void NonPowerOfTwo_ThrowsArgumentException(int capacity)
    {
        Assert.Throws<ArgumentException>(() => Disruptor.CreateSingleProducer<int>(capacity));
        Assert.Throws<ArgumentException>(() => Disruptor.CreateMultiProducer<int>(capacity));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-1024)]
    public void ZeroOrNegative_ThrowsArgumentOutOfRangeException(int capacity)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Disruptor.CreateSingleProducer<int>(capacity));
        Assert.Throws<ArgumentOutOfRangeException>(() => Disruptor.CreateMultiProducer<int>(capacity));
    }

    [Fact]
    public void BatchClaim_ExceedingCapacity_ThrowsArgumentOutOfRangeException()
    {
        var singleRing = Disruptor.CreateSingleProducer<int>(16);
        Assert.Throws<ArgumentOutOfRangeException>(() => singleRing.Claim(17));
        Assert.Throws<ArgumentOutOfRangeException>(() => singleRing.Claim(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => singleRing.Claim(-5));

        var multiRing = Disruptor.CreateMultiProducer<int>(16);
        Assert.Throws<ArgumentOutOfRangeException>(() => multiRing.Claim(17));
        Assert.Throws<ArgumentOutOfRangeException>(() => multiRing.Claim(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => multiRing.Claim(-5));
    }

    [Fact]
    public void IsAvailable_NegativeSequence_ReturnsFalse()
    {
        var singleRing = Disruptor.CreateSingleProducer<int>(16);
        Assert.False(singleRing.Sequencer.IsAvailable(-1));
        Assert.False(singleRing.Sequencer.IsAvailable(-100));

        var multiRing = Disruptor.CreateMultiProducer<int>(16);
        Assert.False(multiRing.Sequencer.IsAvailable(-1));
        Assert.False(multiRing.Sequencer.IsAvailable(-100));
    }

    [Fact]
    public void BatchTryNext_ExceedingCapacity_ReturnsFalse()
    {
        var singleRing = Disruptor.CreateSingleProducer<int>(16);
        Assert.False(singleRing.Sequencer.TryNext(17, out _));
        Assert.False(singleRing.Sequencer.TryNext(0, out _));
        Assert.False(singleRing.Sequencer.TryNext(-1, out _));

        var multiRing = Disruptor.CreateMultiProducer<int>(16);
        Assert.False(multiRing.Sequencer.TryNext(17, out _));
        Assert.False(multiRing.Sequencer.TryNext(0, out _));
        Assert.False(multiRing.Sequencer.TryNext(-1, out _));
    }
}
