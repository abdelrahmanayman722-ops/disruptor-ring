namespace DisruptorRing;

using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
/// Ultra-low latency busy-spin wait strategy that yields zero context switches
/// and sub-microsecond response times by continuously spinning in CPU registers.
/// </summary>
public sealed class BusySpinWaitStrategy : IWaitStrategy
{
    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long WaitFor(long sequence, PaddedSequence cursor)
    {
        long available;
        while ((available = cursor.ReadAcquire()) < sequence)
        {
            Thread.SpinWait(1);
        }
        return available;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SignalAllWhenBlocking()
    {
    }
}
