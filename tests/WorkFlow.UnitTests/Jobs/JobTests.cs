using System.Text.Json;
using WorkFlow.Domain.Jobs;
using Xunit;

namespace WorkFlow.UnitTests.Jobs;

public sealed class JobTests
{
    private static readonly DateTimeOffset Now =
        new(2026, 9, 26, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public void CreateProducesAQueuedJob()
    {
        var job = CreateJob();

        Assert.NotEqual(Guid.Empty, job.Id);
        Assert.Equal(JobState.Queued, job.State);
        Assert.Equal(0, job.AttemptCount);
        Assert.Equal(3, job.MaximumAttempts);
        Assert.Equal(Now, job.AvailableAt);
        Assert.Equal(Now, job.CreatedAt);
        Assert.Equal(Now, job.UpdatedAt);
    }

    [Fact]
    public void CreateRejectsAnEmptyQueue()
    {
        Assert.Throws<ArgumentException>(() => CreateJob(queue: " "));
    }

    [Fact]
    public void CreateRejectsZeroMaximumAttempts()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => CreateJob(maximumAttempts: 0));
    }

    [Fact]
    public void CreateRejectsMalformedJson()
    {
        Assert.ThrowsAny<JsonException>(() => CreateJob(payloadJson: "not-json"));
    }

    private static Job CreateJob(
        string queue = "default",
        int maximumAttempts = 3,
        string payloadJson = "{\"command\":\"example\"}")
    {
        return Job.Create(
            idempotencyKey: "unit-test-key",
            submissionHash: new string('a', 64),
            queue,
            priority: 0,
            payloadJson,
            maximumAttempts,
            Now);
    }
}
