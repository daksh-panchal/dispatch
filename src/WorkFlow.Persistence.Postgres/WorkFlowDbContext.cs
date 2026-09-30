using Microsoft.EntityFrameworkCore;
using WorkFlow.Domain.Jobs;

namespace WorkFlow.Persistence.Postgres;

public sealed class WorkFlowDbContext(DbContextOptions<WorkFlowDbContext> options)
    : DbContext(options)
{
    public DbSet<Job> Jobs => Set<Job>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfigurationsFromAssembly(typeof(WorkFlowDbContext).Assembly);
    }
}
