namespace DisruptorRing;

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

/// <summary>
/// A 64-bit sequence counter with 64-byte cache padding on either side to eliminate
/// false sharing across CPU cores under high-concurrency contention.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = 144)]
public class PaddedSequence
{
    // Leading cache padding (64 bytes, offsets 0..63)
    [FieldOffset(64)]
    private long _value;
    // Trailing cache padding (72 bytes, offsets 72..143)

    /// <summary>
    /// Initializes a new instance of the <see cref="PaddedSequence"/> class.
    /// </summary>
    /// <param name="initialValue">The initial sequence value (defaults to -1L).</param>
    public PaddedSequence(long initialValue = -1L)
    {
        _value = initialValue;
    }

    /// <summary>
    /// Reads the sequence value with acquire memory barrier semantics.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ReadAcquire() => Volatile.Read(ref _value);

    /// <summary>
    /// Writes the sequence value with release memory barrier semantics.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteRelease(long value) => Volatile.Write(ref _value, value);

    /// <summary>
    /// Reads the sequence value without memory barriers.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long ReadOpaque() => _value;

    /// <summary>
    /// Atomically increments the sequence counter by 1.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Increment() => Interlocked.Increment(ref _value);

    /// <summary>
    /// Atomically adds a delta to the sequence counter.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Add(long delta) => Interlocked.Add(ref _value, delta);

    /// <summary>
    /// Atomically compares and swaps the sequence counter.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryCompareExchange(long expected, long newValue)
    {
        return Interlocked.CompareExchange(ref _value, newValue, expected) == expected;
    }

    /// <inheritdoc/>
    public override string ToString() => ReadAcquire().ToString();
}
