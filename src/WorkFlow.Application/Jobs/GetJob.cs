using WorkFlow.Application.Abstractions;

namespace WorkFlow.Application.Jobs;

public sealed class GetJobHandler(IJobRepository repository)
{
    public async Task<JobView?> HandleAsync(Guid id, CancellationToken cancellationToken)
    {
        var job = await repository.FindByIdAsync(id, cancellationToken);
        return job is null ? null : JobView.FromDomain(job);
    }
}
