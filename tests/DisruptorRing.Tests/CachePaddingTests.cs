namespace DisruptorRing.Tests;

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Xunit;

public class CachePaddingTests
{
    [Fact]
    public void RingSlot_Explicit_IsExactly64Bytes()
    {
        int size = Marshal.SizeOf<RingSlot>();
        Assert.Equal(64, size);
        Assert.Equal(64, Unsafe.SizeOf<RingSlot>());
    }

    [Fact]
    public void RingSlot_Int_IsExactly64Bytes()
    {
        int size = Unsafe.SizeOf<RingSlot<int>>();
        Assert.Equal(64, size);
    }

    [Fact]
    public void RingSlot_Long_IsExactly64Bytes()
    {
        int size = Unsafe.SizeOf<RingSlot<long>>();
        Assert.Equal(64, size);
    }

    [Fact]
    public void RingSlot_CustomStruct_IsExactly64Bytes()
    {
        int size = Unsafe.SizeOf<RingSlot<TestPayload>>();
        Assert.Equal(64, size);
    }

    [Fact]
    public void PaddedSequence_HasSufficientCachePadding()
    {
        int size = Marshal.SizeOf<PaddedSequence>();
        // Padded to at least 128 bytes to isolate 64-byte value from adjacent cache lines
        Assert.True(size >= 128);
    }

    public struct TestPayload
    {
        public long Id;
        public double Price;
        public int Volume;
    }
}
