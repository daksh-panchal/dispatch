using System.Text.Json;

namespace WorkFlow.Domain.Jobs;

public sealed class Job
{
    public const int MaximumIdempotencyKeyLength = 200;
    public const int MaximumQueueNameLength = 100;
    public const int MinimumPriority = -1_000;
    public const int MaximumPriority = 1_000;

    private Job()
    {
    }

    private Job(
        Guid id,
        string idempotencyKey,
        string submissionHash,
        string queue,
        int priority,
        string payloadJson,
        int maximumAttempts,
        DateTimeOffset now)
    {
        Id = id;
        IdempotencyKey = idempotencyKey;
        SubmissionHash = submissionHash;
        Queue = queue;
        Priority = priority;
        PayloadJson = payloadJson;
        State = JobState.Queued;
        AttemptCount = 0;
        MaximumAttempts = maximumAttempts;
        AvailableAt = now;
        CreatedAt = now;
        UpdatedAt = now;
    }

    public Guid Id { get; private set; }

    public string IdempotencyKey { get; private set; } = string.Empty;

    public string SubmissionHash { get; private set; } = string.Empty;

    public string Queue { get; private set; } = string.Empty;

    public int Priority { get; private set; }

    public string PayloadJson { get; private set; } = string.Empty;

    public JobState State { get; private set; }

    public int AttemptCount { get; private set; }

    public int MaximumAttempts { get; private set; }

    public DateTimeOffset AvailableAt { get; private set; }

    public DateTimeOffset CreatedAt { get; private set; }

    public DateTimeOffset UpdatedAt { get; private set; }

    public static Job Create(
        string idempotencyKey,
        string submissionHash,
        string queue,
        int priority,
        string payloadJson,
        int maximumAttempts,
        DateTimeOffset now)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(idempotencyKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(submissionHash);
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        ArgumentException.ThrowIfNullOrWhiteSpace(payloadJson);

        idempotencyKey = idempotencyKey.Trim();
        queue = queue.Trim();

        if (idempotencyKey.Length > MaximumIdempotencyKeyLength)
        {
            throw new ArgumentOutOfRangeException(
                nameof(idempotencyKey),
                $"The idempotency key cannot exceed {MaximumIdempotencyKeyLength} characters.");
        }

        if (queue.Length > MaximumQueueNameLength)
        {
            throw new ArgumentOutOfRangeException(
                nameof(queue),
                $"The queue name cannot exceed {MaximumQueueNameLength} characters.");
        }

        if (submissionHash.Length != 64)
        {
            throw new ArgumentException(
                "The submission hash must be a lowercase SHA-256 hex value.",
                nameof(submissionHash));
        }

        if (priority is < MinimumPriority or > MaximumPriority)
        {
            throw new ArgumentOutOfRangeException(
                nameof(priority),
                $"Priority must be between {MinimumPriority} and {MaximumPriority}.");
        }

        if (maximumAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumAttempts),
                "Maximum attempts must be at least one.");
        }

        using var _ = JsonDocument.Parse(payloadJson);
        now = now.ToUniversalTime();

        return new Job(
            Guid.CreateVersion7(),
            idempotencyKey,
            submissionHash,
            queue,
            priority,
            payloadJson,
            maximumAttempts,
            now);
    }
}
