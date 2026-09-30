using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.PostgreSql;
using WorkFlow.Persistence.Postgres;
using Xunit;

namespace WorkFlow.IntegrationTests.Infrastructure;

[SuppressMessage(
    "Design",
    "CA1001:Types that own disposable fields should be disposable",
    Justification = "xUnit invokes IAsyncLifetime.DisposeAsync after the fixture is used.")]
public sealed class WorkFlowApiFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer postgres = new PostgreSqlBuilder("postgres:18.6")
        .WithDatabase("workflow_tests")
        .WithUsername("workflow")
        .WithPassword("workflow_tests")
        .Build();

    private WorkFlowApiFactory? factory;

    public HttpClient Client { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await postgres.StartAsync();

        factory = new WorkFlowApiFactory(postgres.GetConnectionString());

        await using var scope = factory.Services.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<WorkFlowDbContext>();
        await dbContext.Database.MigrateAsync();

        Client = factory.CreateClient();
    }

    public async Task DisposeAsync()
    {
        Client?.Dispose();
        factory?.Dispose();
        await postgres.DisposeAsync();
    }
}
