using Microsoft.EntityFrameworkCore;
using Npgsql;
using WorkFlow.Application.Abstractions;
using WorkFlow.Domain.Jobs;

namespace WorkFlow.Persistence.Postgres;

public sealed class PostgresJobRepository(WorkFlowDbContext dbContext) : IJobRepository
{
    public Task<Job?> FindByIdAsync(Guid id, CancellationToken cancellationToken)
    {
        return dbContext.Jobs
            .AsNoTracking()
            .SingleOrDefaultAsync(job => job.Id == id, cancellationToken);
    }

    public async Task<JobInsertResult> AddOrGetExistingAsync(
        Job job,
        CancellationToken cancellationToken)
    {
        dbContext.Jobs.Add(job);

        try
        {
            await dbContext.SaveChangesAsync(cancellationToken);
            return new JobInsertResult(job, WasCreated: true);
        }
        catch (DbUpdateException exception)
            when (IsIdempotencyKeyConflict(exception))
        {
            dbContext.ChangeTracker.Clear();

            var existing = await dbContext.Jobs
                .AsNoTracking()
                .SingleAsync(
                    candidate => candidate.IdempotencyKey == job.IdempotencyKey,
                    cancellationToken);

            return new JobInsertResult(existing, WasCreated: false);
        }
    }

    private static bool IsIdempotencyKeyConflict(DbUpdateException exception)
    {
        return exception.InnerException is PostgresException
        {
            SqlState: PostgresErrorCodes.UniqueViolation,
            ConstraintName: "ux_jobs_idempotency_key"
        };
    }
}
