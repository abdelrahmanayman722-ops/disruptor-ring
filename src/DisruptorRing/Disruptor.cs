namespace DisruptorRing;

/// <summary>
/// Static entry point and factory for creating high-performance <see cref="RingBuffer{T}"/> instances.
/// </summary>
public static class Disruptor
{
    /// <summary>
    /// Creates a single-producer ring buffer optimized for maximum throughput with zero CAS contention.
    /// </summary>
    /// <typeparam name="T">The unmanaged payload type.</typeparam>
    /// <param name="capacity">Buffer size (must be a power of two, e.g. 1024, 65536).</param>
    /// <param name="waitStrategy">Optional custom wait strategy.</param>
    /// <returns>A configured <see cref="RingBuffer{T}"/>.</returns>
    public static RingBuffer<T> CreateSingleProducer<T>(int capacity, IWaitStrategy? waitStrategy = null) where T : unmanaged
    {
        var sequencer = new SingleProducerSequencer(capacity, waitStrategy);
        return new RingBuffer<T>(sequencer);
    }

    /// <summary>
    /// Creates a multi-producer ring buffer coordinating concurrent producers via lock-free atomic CAS sequencers.
    /// </summary>
    /// <typeparam name="T">The unmanaged payload type.</typeparam>
    /// <param name="capacity">Buffer size (must be a power of two, e.g. 1024, 65536).</param>
    /// <param name="waitStrategy">Optional custom wait strategy.</param>
    /// <returns>A configured <see cref="RingBuffer{T}"/>.</returns>
    public static RingBuffer<T> CreateMultiProducer<T>(int capacity, IWaitStrategy? waitStrategy = null) where T : unmanaged
    {
        var sequencer = new MultiProducerSequencer(capacity, waitStrategy);
        return new RingBuffer<T>(sequencer);
    }
}
