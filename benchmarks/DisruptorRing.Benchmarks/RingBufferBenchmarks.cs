namespace DisruptorRing.Benchmarks;

using System.Collections.Concurrent;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using DisruptorRing;

[SimpleJob(RuntimeMoniker.Net90)]
[MemoryDiagnoser]
public class RingBufferBenchmarks
{
    private const int BufferCapacity = 65536;
    private const int OperationsCount = 100_000;

    private RingBuffer<long> _singleProducerRing = null!;
    private RingBuffer<long> _multiProducerRing = null!;
    private Channel<long> _channel = null!;
    private ConcurrentQueue<long> _queue = null!;

    [GlobalSetup]
    public void Setup()
    {
        _singleProducerRing = Disruptor.CreateSingleProducer<long>(BufferCapacity);
        _multiProducerRing = Disruptor.CreateMultiProducer<long>(BufferCapacity);

        _channel = Channel.CreateBounded<long>(new BoundedChannelOptions(BufferCapacity)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });

        _queue = new ConcurrentQueue<long>();
    }

    [Benchmark(Baseline = true)]
    public void SingleProducerDisruptorRing_PublishConsume()
    {
        for (int i = 0; i < OperationsCount; i++)
        {
            _singleProducerRing.Publish(i);
            _singleProducerRing.TryConsume(out _);
        }
    }

    [Benchmark]
    public void MultiProducerDisruptorRing_PublishConsume()
    {
        for (int i = 0; i < OperationsCount; i++)
        {
            _multiProducerRing.Publish(i);
            _multiProducerRing.TryConsume(out _);
        }
    }

    [Benchmark]
    public void DotNetChannel_PublishConsume()
    {
        var writer = _channel.Writer;
        var reader = _channel.Reader;
        for (int i = 0; i < OperationsCount; i++)
        {
            writer.TryWrite(i);
            reader.TryRead(out _);
        }
    }

    [Benchmark]
    public void ConcurrentQueue_EnqueueDequeue()
    {
        for (int i = 0; i < OperationsCount; i++)
        {
            _queue.Enqueue(i);
            _queue.TryDequeue(out _);
        }
    }
}
