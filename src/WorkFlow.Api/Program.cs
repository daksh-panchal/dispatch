using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using WorkFlow.Api.Endpoints;
using WorkFlow.Api.Health;
using WorkFlow.Application.Abstractions;
using WorkFlow.Application.Jobs;
using WorkFlow.Persistence.Postgres;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "HH:mm:ss ";
});

builder.Services.AddProblemDetails();
builder.Services.AddOpenApi("v1");
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    options.SerializerOptions.Converters.Add(
        new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
});

var connectionString = builder.Configuration.GetConnectionString("WorkFlow")
    ?? throw new InvalidOperationException(
        "The ConnectionStrings:WorkFlow configuration value is required.");

var dataSource = new NpgsqlDataSourceBuilder(connectionString).Build();
builder.Services.AddSingleton(dataSource);
builder.Services.AddDbContext<WorkFlowDbContext>(options => options.UseNpgsql(dataSource));

builder.Services.AddScoped<IJobRepository, PostgresJobRepository>();
builder.Services.AddScoped<SubmitJobHandler>();
builder.Services.AddScoped<GetJobHandler>();
builder.Services.AddSingleton(TimeProvider.System);

builder.Services
    .AddHealthChecks()
    .AddCheck<PostgresHealthCheck>("postgres", tags: ["ready"]);

var app = builder.Build();

app.UseExceptionHandler();

app.MapOpenApi("/openapi/{documentName}.json");
app.MapHealthChecks("/health/live", new HealthCheckOptions
{
    Predicate = _ => false
});
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = registration => registration.Tags.Contains("ready")
});

app.MapJobEndpoints();

app.Run();

public partial class Program;
