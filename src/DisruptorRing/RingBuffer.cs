namespace DisruptorRing;

using System;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
/// Ultra-low-latency, zero-allocation lock-free ring buffer built with cache-line aligned slots
/// (64-byte false-sharing elimination) and sequential consistency semantics.
/// </summary>
/// <typeparam name="T">The unmanaged payload type.</typeparam>
public sealed class RingBuffer<T> where T : unmanaged
{
    private readonly RingSlot<T>[] _slots;
    private readonly int _indexMask;
    private readonly ISequencer _sequencer;
    private readonly PaddedSequence _consumerSequence = new(-1L);

    /// <summary>
    /// Gets the ring buffer capacity (guaranteed power of two).
    /// </summary>
    public int Capacity => _sequencer.BufferSize;

    /// <summary>
    /// Gets the current highest published sequence.
    /// </summary>
    public long Cursor => _sequencer.Cursor;

    /// <summary>
    /// Gets the current consumer sequence.
    /// </summary>
    public long ConsumerSequence => _consumerSequence.ReadAcquire();

    /// <summary>
    /// Gets a value indicating whether the ring buffer is currently empty.
    /// </summary>
    public bool IsEmpty => _consumerSequence.ReadAcquire() >= _sequencer.Cursor;

    /// <summary>
    /// Gets the sequencer coordinating buffer claims.
    /// </summary>
    public ISequencer Sequencer => _sequencer;

    /// <summary>
    /// Initializes a new instance of the <see cref="RingBuffer{T}"/> class.
    /// </summary>
    /// <param name="sequencer">The sequencer managing concurrent claims.</param>
    public RingBuffer(ISequencer sequencer)
    {
        _sequencer = sequencer ?? throw new ArgumentNullException(nameof(sequencer));
        int capacity = sequencer.BufferSize;
        _indexMask = capacity - 1;

        // Pre-allocate continuous cache-line aligned slot array
        _slots = new RingSlot<T>[capacity];
        for (int i = 0; i < capacity; i++)
        {
            _slots[i] = new RingSlot<T>(-1L, default);
        }

        // Register consumer sequence as gating sequence on sequencer
        _sequencer.AddGatingSequence(_consumerSequence);
    }

    /// <summary>
    /// Claims the next slot and publishes <paramref name="item"/> into the ring buffer.
    /// Spins if the ring buffer is temporarily saturated.
    /// </summary>
    /// <param name="item">The value to publish.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Publish(in T item)
    {
        long sequence = _sequencer.Next();
        int index = (int)(sequence & _indexMask);

        _slots[index].Value = item;
        _slots[index].Sequence = sequence;

        _sequencer.Publish(sequence);
    }

    /// <summary>
    /// Attempts to publish <paramref name="item"/> without blocking.
    /// Returns <c>false</c> immediately if the ring buffer is currently full.
    /// </summary>
    /// <param name="item">The value to publish.</param>
    /// <returns><c>true</c> if successfully published; otherwise, <c>false</c>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryPublish(in T item)
    {
        if (!_sequencer.TryNext(out long sequence))
        {
            return false;
        }

        int index = (int)(sequence & _indexMask);
        _slots[index].Value = item;
        _slots[index].Sequence = sequence;

        _sequencer.Publish(sequence);
        return true;
    }

    /// <summary>
    /// Claims a sequence number for writing directly by reference.
    /// Must be followed by <see cref="Commit(long)"/> once populated.
    /// </summary>
    /// <returns>The claimed sequence.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Claim() => _sequencer.Next();

    /// <summary>
    /// Returns a direct reference to the slot payload at the claimed <paramref name="sequence"/>,
    /// enabling zero-copy in-place mutations.
    /// </summary>
    /// <param name="sequence">The sequence claimed via <see cref="Claim"/>.</param>
    /// <returns>Reference to the slot payload.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ref T Get(long sequence)
    {
        int index = (int)(sequence & _indexMask);
        return ref _slots[index].Value;
    }

    /// <summary>
    /// Commits the specified <paramref name="sequence"/>, making it available to consumers.
    /// </summary>
    /// <param name="sequence">The sequence previously claimed.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Commit(long sequence)
    {
        int index = (int)(sequence & _indexMask);
        _slots[index].Sequence = sequence;
        _sequencer.Publish(sequence);
    }

    /// <summary>
    /// Attempts to consume the next available item from the ring buffer.
    /// </summary>
    /// <param name="item">The consumed value if available.</param>
    /// <returns><c>true</c> if an item was consumed; <c>false</c> if the buffer is empty.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryConsume(out T item)
    {
        long currentConsumer = _consumerSequence.ReadAcquire();
        long nextSequence = currentConsumer + 1;
        long cursor = _sequencer.Cursor;

        if (nextSequence > cursor)
        {
            item = default;
            return false;
        }

        // For multi-producer sequencers, verify slot readiness
        if (_sequencer is MultiProducerSequencer multi && !multi.IsAvailable(nextSequence))
        {
            item = default;
            return false;
        }

        int index = (int)(nextSequence & _indexMask);
        item = _slots[index].Value;

        _consumerSequence.WriteRelease(nextSequence);
        return true;
    }

    /// <summary>
    /// Consumes the next item, spinning with <see cref="Thread.SpinWait"/> until one becomes available.
    /// </summary>
    /// <returns>The consumed item.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T Consume()
    {
        long currentConsumer = _consumerSequence.ReadAcquire();
        long nextSequence = currentConsumer + 1;

        while (nextSequence > _sequencer.Cursor)
        {
            Thread.SpinWait(1);
        }

        if (_sequencer is MultiProducerSequencer multi)
        {
            while (!multi.IsAvailable(nextSequence))
            {
                Thread.SpinWait(1);
            }
        }

        int index = (int)(nextSequence & _indexMask);
        T item = _slots[index].Value;

        _consumerSequence.WriteRelease(nextSequence);
        return item;
    }

    /// <summary>
    /// Gets the number of items currently available to be consumed.
    /// </summary>
    public int AvailableCount
    {
        get
        {
            long produced = _sequencer.Cursor;
            long consumed = _consumerSequence.ReadAcquire();
            long diff = produced - consumed;
            return diff > 0 ? (int)diff : 0;
        }
    }
}
