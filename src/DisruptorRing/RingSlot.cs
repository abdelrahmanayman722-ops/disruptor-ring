namespace DisruptorRing;

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

/// <summary>
/// Cache-line aligned slot container explicitly sized to 64 bytes via explicit layout
/// to completely eliminate false sharing across CPU cores in high-throughput pipelines.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 64)]
public struct RingSlot
{
    /// <summary>
    /// The sequence number assigned to this slot.
    /// </summary>
    [FieldOffset(0)]
    public long Sequence;

    /// <summary>
    /// The 64-bit value payload stored within this slot.
    /// </summary>
    [FieldOffset(8)]
    public long Value;

    [FieldOffset(16)]
    private long _pad0;

    [FieldOffset(24)]
    private long _pad1;

    [FieldOffset(32)]
    private long _pad2;

    [FieldOffset(40)]
    private long _pad3;

    [FieldOffset(48)]
    private long _pad4;

    [FieldOffset(56)]
    private long _pad5;

    /// <summary>
    /// Initializes a new instance of the <see cref="RingSlot"/> struct.
    /// </summary>
    /// <param name="sequence">The sequence number.</param>
    /// <param name="value">The 64-bit value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public RingSlot(long sequence, long value)
    {
        Sequence = sequence;
        Value = value;
        _pad0 = _pad1 = _pad2 = _pad3 = _pad4 = _pad5 = 0;
    }
}

/// <summary>
/// Cache-line aligned generic slot container explicitly sized to 64 bytes
/// to eliminate false sharing between adjacent slots across CPU cache lines.
/// </summary>
/// <typeparam name="T">The value type payload, constrained to unmanaged for zero-allocation and Native AOT compatibility.</typeparam>
[StructLayout(LayoutKind.Sequential, Size = 64)]
public struct RingSlot<T> where T : unmanaged
{
    /// <summary>
    /// The sequence number assigned to this slot, indicating state progression.
    /// </summary>
    public long Sequence;

    /// <summary>
    /// The value payload stored within this slot.
    /// </summary>
    public T Value;

    /// <summary>
    /// Initializes a new instance of the <see cref="RingSlot{T}"/> struct.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public RingSlot(long sequence, in T value)
    {
        Sequence = sequence;
        Value = value;
    }
}
