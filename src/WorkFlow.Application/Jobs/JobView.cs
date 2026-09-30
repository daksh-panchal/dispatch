using System.Text.Json;
using WorkFlow.Domain.Jobs;

namespace WorkFlow.Application.Jobs;

public sealed record JobView(
    Guid Id,
    string Queue,
    int Priority,
    JsonElement Payload,
    JobState State,
    int AttemptCount,
    int MaximumAttempts,
    DateTimeOffset AvailableAt,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt)
{
    public static JobView FromDomain(Job job)
    {
        using var document = JsonDocument.Parse(job.PayloadJson);

        return new JobView(
            job.Id,
            job.Queue,
            job.Priority,
            document.RootElement.Clone(),
            job.State,
            job.AttemptCount,
            job.MaximumAttempts,
            job.AvailableAt,
            job.CreatedAt,
            job.UpdatedAt);
    }
}
