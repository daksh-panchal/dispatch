using WorkFlow.Domain.Jobs;

namespace WorkFlow.Application.Abstractions;

public interface IJobRepository
{
    Task<Job?> FindByIdAsync(Guid id, CancellationToken cancellationToken);

    Task<JobInsertResult> AddOrGetExistingAsync(Job job, CancellationToken cancellationToken);
}

public sealed record JobInsertResult(Job Job, bool WasCreated);
