namespace DisruptorRing;

using System;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
/// High-throughput single-producer sequencer. Completely eliminates CAS atomic operations
/// for the producer, caching downstream consumer sequences to minimize cross-core bus traffic.
/// </summary>
public sealed class SingleProducerSequencer : ISequencer
{
    private readonly int _bufferSize;
    private readonly IWaitStrategy _waitStrategy;
    private readonly PaddedSequence _cursor = new(-1L);
    private long _nextValue = -1L;
    private long _cachedGatingSequence = -1L;

    // Gating sequences from downstream consumers
    private PaddedSequence[] _gatingSequences = Array.Empty<PaddedSequence>();
    private readonly object _lock = new();

    /// <inheritdoc/>
    public int BufferSize => _bufferSize;

    /// <inheritdoc/>
    public long Cursor => _cursor.ReadAcquire();

    /// <summary>
    /// Initializes a new instance of the <see cref="SingleProducerSequencer"/> class.
    /// </summary>
    /// <param name="bufferSize">The ring buffer size, must be a positive power of two.</param>
    /// <param name="waitStrategy">The wait strategy for downstream consumers.</param>
    public SingleProducerSequencer(int bufferSize, IWaitStrategy? waitStrategy = null)
    {
        if (bufferSize < 1)
            throw new ArgumentOutOfRangeException(nameof(bufferSize), "Buffer size must be at least 1.");

        if ((bufferSize & (bufferSize - 1)) != 0)
            throw new ArgumentException("Buffer size must be a power of two.", nameof(bufferSize));

        _bufferSize = bufferSize;
        _waitStrategy = waitStrategy ?? new BusySpinWaitStrategy();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Next() => Next(1);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long Next(int n)
    {
        if (n < 1)
            throw new ArgumentOutOfRangeException(nameof(n), "Claim batch size must be >= 1.");

        long nextValue = _nextValue + n;
        long wrapPoint = nextValue - _bufferSize;
        long cachedGating = _cachedGatingSequence;

        if (wrapPoint > cachedGating || cachedGating > _nextValue)
        {
            long minGating;
            while (wrapPoint > (minGating = GetMinimumGatingSequence()))
            {
                Thread.SpinWait(1);
            }
            _cachedGatingSequence = minGating;
        }

        _nextValue = nextValue;
        return nextValue;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryNext(out long sequence)
    {
        long nextValue = _nextValue + 1;
        long wrapPoint = nextValue - _bufferSize;
        long cachedGating = _cachedGatingSequence;

        if (wrapPoint > cachedGating || cachedGating > _nextValue)
        {
            long minGating = GetMinimumGatingSequence();
            if (wrapPoint > minGating)
            {
                sequence = -1L;
                return false;
            }
            _cachedGatingSequence = minGating;
        }

        _nextValue = nextValue;
        sequence = nextValue;
        return true;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Publish(long sequence)
    {
        _cursor.WriteRelease(sequence);
        _waitStrategy.SignalAllWhenBlocking();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool HasAvailableCapacity(int requiredCapacity)
    {
        long nextValue = _nextValue + requiredCapacity;
        long wrapPoint = nextValue - _bufferSize;
        long cachedGating = _cachedGatingSequence;

        if (wrapPoint > cachedGating || cachedGating > _nextValue)
        {
            long minGating = GetMinimumGatingSequence();
            _cachedGatingSequence = minGating;
            if (wrapPoint > minGating)
            {
                return false;
            }
        }
        return true;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long GetRemainingCapacity()
    {
        long consumed = GetMinimumGatingSequence();
        long produced = _nextValue;
        return _bufferSize - (produced - consumed);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long GetMinimumGatingSequence()
    {
        var gating = _gatingSequences;
        if (gating.Length == 0)
            return _cursor.ReadAcquire();

        long min = long.MaxValue;
        for (int i = 0; i < gating.Length; i++)
        {
            long seq = gating[i].ReadAcquire();
            if (seq < min)
                min = seq;
        }
        return min;
    }

    /// <inheritdoc/>
    public void AddGatingSequence(PaddedSequence gatingSequence)
    {
        lock (_lock)
        {
            int oldLen = _gatingSequences.Length;
            var updated = new PaddedSequence[oldLen + 1];
            Array.Copy(_gatingSequences, updated, oldLen);
            updated[oldLen] = gatingSequence;
            _gatingSequences = updated;
        }
    }
}
