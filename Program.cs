using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace DataflowHang
{
    class JobThing
    {
        public string Id { get; set; }
    }

    class JobOptions
    {
        public int batchSize { get; set; }
        public int ParallelMaxTasks { get; set; }
    }

    class Job
    {
        private readonly JobOptions _options = new JobOptions
        {
            ParallelMaxTasks = 4,
            batchSize = 50
        };

        private readonly ExecutionDataflowBlockOptions _blockOptions;

        public Job()
        {
            _blockOptions = new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = _options.ParallelMaxTasks };
        }

        private Task DummyBatchAction(JobThing[] things)
        {
            return Task.CompletedTask;
        }

        private Task<JobThing> DummyTransformAction(JobThing thing)
        {
            return Task.FromResult(thing);
        }

        public async Task Run(CancellationToken cancellationToken)
        {
            try
            {
                var fetchBlock = new TransformBlock<JobThing, JobThing>(
                    DummyTransformAction,
                    new ExecutionDataflowBlockOptions
                    {
                        CancellationToken = cancellationToken,
                        MaxDegreeOfParallelism = _options.ParallelMaxTasks,
                        BoundedCapacity = _options.batchSize * _options.ParallelMaxTasks * 2
                    }
                );

                var batchBlock = new BatchBlock<JobThing>(
                    _options.batchSize,
                    new GroupingDataflowBlockOptions
                    {
                        BoundedCapacity = _options.batchSize * _options.ParallelMaxTasks * 2,
                        CancellationToken = cancellationToken
                    });

                var importAppsBlock = new ActionBlock<JobThing[]>(
                    DummyBatchAction, _blockOptions);

                var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };
                fetchBlock.LinkTo(batchBlock, linkOptions, a => a != null);
                fetchBlock.LinkTo(DataflowBlock.NullTarget<JobThing>(), linkOptions);
                batchBlock.LinkTo(importAppsBlock, linkOptions);

                bool sendResult = true;
                for (int i = 0; i < 5000; ++i)
                {
                    var thing = new JobThing { Id = $"thing {i}" };
                    sendResult &= await fetchBlock.SendAsync(thing, cancellationToken);
                }

                Console.WriteLine("processing complete");
                fetchBlock.Complete();
                Console.WriteLine("waiting for dataflow to complete");
                await importAppsBlock.Completion;
                Console.WriteLine("dataflow completed");

                // This shouldn't happen -- SendAsync should only fail if the pipeline
                // has failed, which will cause the await block above to throw. Sanity
                // check it anyway.
                if (!sendResult)
                {
                    throw new Exception("SendAsync unexpectedly failed");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"caught unexpected exception {e}");
                throw;
            }
        }
    }

    class Program
    {
        static private readonly CancellationTokenSource _cts = new CancellationTokenSource();


        static async Task TryMain(string[] args)
        {
            var done = new ManualResetEventSlim(false);
            try
            {
                void Shutdown()
                {
                    try
                    {
                        _cts.Cancel();
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                    done.Wait();
                };

                AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => Shutdown();
                Console.CancelKeyPress += (sender, eventArgs) =>
                {
                    Shutdown();
                    // Don't terminate the process immediately, wait for the Main thread to exit gracefully.
                    eventArgs.Cancel = true;
                };

                var job = new Job();
                for (int i = 0; i < 5000; ++i)
                {
                    await job.Run(_cts.Token);
                }
            }
            finally
            {
                done.Set();
            }
        }

        static async Task<int> Main(string[] args)
        {
            try
            {
                await TryMain(args);
                return 0;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Caught exception: {e}");
                return 1;
            }
        }
    }
}
