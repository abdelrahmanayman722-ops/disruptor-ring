namespace DisruptorRing;

using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
/// A balanced wait strategy that initially spins with <see cref="Thread.SpinWait"/>,
/// and then yields the CPU timeslice using <see cref="Thread.Yield"/> if contention persists.
/// </summary>
public sealed class YieldingWaitStrategy : IWaitStrategy
{
    private const int SpinTries = 100;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long WaitFor(long sequence, PaddedSequence cursor)
    {
        int counter = SpinTries;
        long available;

        while ((available = cursor.ReadAcquire()) < sequence)
        {
            if (counter > 0)
            {
                counter--;
                Thread.SpinWait(1);
            }
            else
            {
                Thread.Yield();
            }
        }

        return available;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SignalAllWhenBlocking()
    {
    }
}
