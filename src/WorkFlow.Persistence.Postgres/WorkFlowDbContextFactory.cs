using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace WorkFlow.Persistence.Postgres;

public sealed class WorkFlowDbContextFactory : IDesignTimeDbContextFactory<WorkFlowDbContext>
{
    public WorkFlowDbContext CreateDbContext(string[] args)
    {
        const string developmentConnectionString =
            "Host=localhost;Port=5432;Database=workflow;Username=workflow;Password=workflow_dev";

        var connectionString = Environment.GetEnvironmentVariable("WORKFLOW_POSTGRES_CONNECTION")
            ?? developmentConnectionString;

        var options = new DbContextOptionsBuilder<WorkFlowDbContext>()
            .UseNpgsql(connectionString)
            .Options;

        return new WorkFlowDbContext(options);
    }
}
