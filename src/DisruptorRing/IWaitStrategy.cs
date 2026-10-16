namespace DisruptorRing;

/// <summary>
/// Strategy used by consumers or producers when waiting for sequences to become available.
/// </summary>
public interface IWaitStrategy
{
    /// <summary>
    /// Waits for the specified sequence to become available from the target cursor.
    /// </summary>
    /// <param name="sequence">The sequence being waited upon.</param>
    /// <param name="cursor">The cursor sequence being monitored.</param>
    /// <returns>The highest available sequence.</returns>
    long WaitFor(long sequence, PaddedSequence cursor);

    /// <summary>
    /// Signals all waiting threads that an update occurred (noop for non-blocking strategies).
    /// </summary>
    void SignalAllWhenBlocking();
}
