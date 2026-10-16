namespace DisruptorRing;

using System;
using System.Runtime.CompilerServices;
using System.Threading;

/// <summary>
/// High-performance multi-producer sequencer employing lock-free atomic CAS loops
/// for sequence allocation, supporting concurrent producer threads with sequential consistency.
/// </summary>
public sealed class MultiProducerSequencer : ISequencer
{
    private readonly int _bufferSize;
    private readonly int _indexMask;
    private readonly int _indexShift;
    private readonly int[] _availableBuffer;
    private readonly IWaitStrategy _waitStrategy;

    private readonly PaddedSequence _cursor = new(-1L);
    private readonly PaddedSequence _cachedGatingSequence = new(-1L);
    private PaddedSequence[] _gatingSequences = Array.Empty<PaddedSequence>();
    private readonly object _lock = new();

    /// <inheritdoc/>
    public int BufferSize => _bufferSize;

    /// <inheritdoc/>
    public long Cursor => _cursor.ReadAcquire();

    /// <summary>
    /// Initializes a new instance of the <see cref="MultiProducerSequencer"/> class.
    /// </summary>
    /// <param name="bufferSize">The ring buffer size, must be a positive power of two.</param>
    /// <param name="waitStrategy">The wait strategy for downstream consumers.</param>
    public MultiProducerSequencer(int bufferSize, IWaitStrategy? waitStrategy = null)
    {
        if (bufferSize < 1)
            throw new ArgumentOutOfRangeException(nameof(bufferSize), "Buffer size must be at least 1.");

        if ((bufferSize & (bufferSize - 1)) != 0)
            throw new ArgumentException("Buffer size must be a power of two.", nameof(bufferSize));

        _bufferSize = bufferSize;
        _indexMask = bufferSize - 1;
        _indexShift = Log2(bufferSize);
        _waitStrategy = waitStrategy ?? new BusySpinWaitStrategy();

        _availableBuffer = new int[bufferSize];
        Array.Fill(_availableBuffer, -1);
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
        if (n > _bufferSize)
            throw new ArgumentOutOfRangeException(nameof(n), $"Claim batch size {n} cannot exceed buffer size {_bufferSize}.");

        long current;
        long next;

        do
        {
            current = _cursor.ReadAcquire();
            next = current + n;

            long wrapPoint = next - _bufferSize;
            long cachedGating = _cachedGatingSequence.ReadAcquire();

            if (wrapPoint > cachedGating || cachedGating > current)
            {
                long minGating = GetMinimumGatingSequence();
                if (wrapPoint > minGating)
                {
                    Thread.SpinWait(1);
                    continue;
                }
                _cachedGatingSequence.WriteRelease(minGating);
            }

            if (_cursor.TryCompareExchange(current, next))
            {
                break;
            }
        } while (true);

        return next;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryNext(out long sequence) => TryNext(1, out sequence);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryNext(int n, out long sequence)
    {
        if (n < 1 || n > _bufferSize)
        {
            sequence = -1L;
            return false;
        }

        long current;
        long next;

        do
        {
            current = _cursor.ReadAcquire();
            next = current + n;

            long wrapPoint = next - _bufferSize;
            long cachedGating = _cachedGatingSequence.ReadAcquire();

            if (wrapPoint > cachedGating || cachedGating > current)
            {
                long minGating = GetMinimumGatingSequence();
                if (wrapPoint > minGating)
                {
                    sequence = -1L;
                    return false;
                }
                _cachedGatingSequence.WriteRelease(minGating);
            }

            if (_cursor.TryCompareExchange(current, next))
            {
                sequence = next;
                return true;
            }
        } while (true);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Publish(long sequence)
    {
        SetAvailable(sequence);
        _waitStrategy.SignalAllWhenBlocking();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Publish(long lo, long hi)
    {
        for (long l = lo; l <= hi; l++)
        {
            SetAvailable(l);
        }
        _waitStrategy.SignalAllWhenBlocking();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsAvailable(long sequence)
    {
        if (sequence < 0)
            return false;

        if (sequence > _cursor.ReadAcquire())
            return false;

        int index = (int)(sequence & _indexMask);
        int flag = (int)(sequence >> _indexShift);
        return Volatile.Read(ref _availableBuffer[index]) == flag;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long WaitFor(long sequence)
    {
        long available = _waitStrategy.WaitFor(sequence, _cursor);
        if (available < sequence)
            return available;

        while (!IsAvailable(sequence))
        {
            Thread.SpinWait(1);
        }
        return sequence;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long GetHighestPublishedSequence(long lowerBound, long availableSequence)
    {
        for (long seq = lowerBound; seq <= availableSequence; seq++)
        {
            if (!IsAvailable(seq))
            {
                return seq - 1;
            }
        }
        return availableSequence;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool HasAvailableCapacity(int requiredCapacity)
    {
        long cursor = _cursor.ReadAcquire();
        long wrapPoint = (cursor + requiredCapacity) - _bufferSize;
        long cachedGating = _cachedGatingSequence.ReadAcquire();

        if (wrapPoint > cachedGating || cachedGating > cursor)
        {
            long minGating = GetMinimumGatingSequence();
            _cachedGatingSequence.WriteRelease(minGating);
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
        long produced = _cursor.ReadAcquire();
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SetAvailable(long sequence)
    {
        int index = (int)(sequence & _indexMask);
        int flag = (int)(sequence >> _indexShift);
        Volatile.Write(ref _availableBuffer[index], flag);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Log2(int value)
    {
        return 31 - System.Numerics.BitOperations.LeadingZeroCount((uint)value);
    }
}
