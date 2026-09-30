using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using WorkFlow.Application.Jobs;

namespace WorkFlow.Api.Endpoints;

public static class JobEndpoints
{
    public static IEndpointRouteBuilder MapJobEndpoints(this IEndpointRouteBuilder endpoints)
    {
        var group = endpoints
            .MapGroup("/api/v1/jobs")
            .WithTags("Jobs");

        group.MapPost("/", SubmitJobAsync)
            .WithName("SubmitJob")
            .Produces<JobView>(StatusCodes.Status201Created) // the job already exists, so return the existing job
            .Produces<JobView>(StatusCodes.Status200OK) // the job doesn't already exist, so create a new job
            .ProducesProblem(StatusCodes.Status400BadRequest) // invalid request (missing idempotency key, missing payload, invalid payload)
            .ProducesProblem(StatusCodes.Status409Conflict); // idempotency key has already been used for a different job 

        group.MapGet("/{jobId:guid}", GetJobAsync)
            .WithName("GetJob")
            .Produces<JobView>()
            .ProducesProblem(StatusCodes.Status404NotFound); // the requested job does not exist

        return endpoints;
    }

    private static async Task<IResult> SubmitJobAsync(
        [FromHeader(Name = "Idempotency-Key")] string? idempotencyKey,
        SubmitJobRequest request,
        SubmitJobHandler handler,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(idempotencyKey))
        {
            return Results.Problem(
                statusCode: StatusCodes.Status400BadRequest,
                title: "An Idempotency-Key header is required.");
        }

        if (request.Payload.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null)
        {
            return Results.Problem(
                statusCode: StatusCodes.Status400BadRequest,
                title: "A JSON payload is required.");
        }

        try
        {
            var result = await handler.HandleAsync(
                new SubmitJobCommand(
                    idempotencyKey,
                    request.Queue,
                    request.Priority,
                    request.MaximumAttempts,
                    request.Payload),
                cancellationToken);

            return result.Outcome switch
            {
                SubmitJobOutcome.Created => Results.Created(
                    $"/api/v1/jobs/{result.Job.Id}",
                    result.Job),
                SubmitJobOutcome.Existing => Results.Ok(result.Job),
                SubmitJobOutcome.IdempotencyConflict => Results.Problem(
                    statusCode: StatusCodes.Status409Conflict,
                    title: "The idempotency key has already been used for a different request."),
                _ => throw new InvalidOperationException("Unknown submission outcome.")
            };
        }
        catch (ArgumentException exception)
        {
            return Results.Problem(
                statusCode: StatusCodes.Status400BadRequest,
                title: "The job submission is invalid.",
                detail: exception.Message);
        }
        catch (JsonException exception)
        {
            return Results.Problem(
                statusCode: StatusCodes.Status400BadRequest,
                title: "The job payload is invalid JSON.",
                detail: exception.Message);
        }
    }

    private static async Task<IResult> GetJobAsync(
        Guid jobId,
        GetJobHandler handler,
        CancellationToken cancellationToken)
    {
        var job = await handler.HandleAsync(jobId, cancellationToken);

        return job is null
            ? Results.Problem(
                statusCode: StatusCodes.Status404NotFound,
                title: "The requested job does not exist.")
            : Results.Ok(job);
    }
}

public sealed record SubmitJobRequest(
    string Queue,
    int Priority,
    int MaximumAttempts,
    JsonElement Payload);
