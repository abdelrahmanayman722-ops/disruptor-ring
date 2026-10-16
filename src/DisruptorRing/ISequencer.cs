namespace DisruptorRing;

/// <summary>
/// Coordinates sequencing and claim bounds across producers and consumers in the ring buffer.
/// </summary>
public interface ISequencer
{
    /// <summary>
    /// Gets the ring buffer capacity (strictly a power of two).
    /// </summary>
    int BufferSize { get; }

    /// <summary>
    /// Gets the current published cursor sequence.
    /// </summary>
    long Cursor { get; }

    /// <summary>
    /// Claims the next sequence number for writing. Blocks/spins if the ring is full.
    /// </summary>
    /// <returns>The claimed sequence number.</returns>
    long Next();

    /// <summary>
    /// Claims the next batch of <paramref name="n"/> sequence numbers for writing.
    /// </summary>
    /// <param name="n">Number of slots to claim.</param>
    /// <returns>The highest claimed sequence number in the batch.</returns>
    long Next(int n);

    /// <summary>
    /// Attempts to claim the next sequence number without blocking.
    /// </summary>
    /// <param name="sequence">The claimed sequence if successful.</param>
    /// <returns><c>true</c> if a slot was successfully claimed; <c>false</c> if the buffer is full.</returns>
    bool TryNext(out long sequence);

    /// <summary>
    /// Publishes the specified sequence number, making it visible to downstream consumers.
    /// </summary>
    /// <param name="sequence">The sequence number to publish.</param>
    void Publish(long sequence);

    /// <summary>
    /// Checks whether the buffer has available capacity for <paramref name="requiredCapacity"/> slots.
    /// </summary>
    /// <param name="requiredCapacity">The number of slots required.</param>
    /// <returns><c>true</c> if capacity is available; otherwise, <c>false</c>.</returns>
    bool HasAvailableCapacity(int requiredCapacity);

    /// <summary>
    /// Computes the remaining unallocated capacity in the ring buffer.
    /// </summary>
    long GetRemainingCapacity();

    /// <summary>
    /// Determines the minimum gating sequence across all registered downstream consumers.
    /// </summary>
    long GetMinimumGatingSequence();

    /// <summary>
    /// Adds a gating sequence representing a consumer to prevent slot overwrite before consumption.
    /// </summary>
    /// <param name="gatingSequence">The consumer sequence.</param>
    void AddGatingSequence(PaddedSequence gatingSequence);
}
