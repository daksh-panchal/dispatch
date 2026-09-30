using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using WorkFlow.Application.Abstractions;
using WorkFlow.Domain.Jobs;

namespace WorkFlow.Application.Jobs;

public sealed record SubmitJobCommand(
    string IdempotencyKey,
    string Queue,
    int Priority,
    int MaximumAttempts,
    JsonElement Payload);

public enum SubmitJobOutcome
{
    Created,
    Existing,
    IdempotencyConflict
}

public sealed record SubmitJobResult(SubmitJobOutcome Outcome, JobView Job);

public sealed class SubmitJobHandler(IJobRepository repository, TimeProvider timeProvider)
{
    public async Task<SubmitJobResult> HandleAsync(
        SubmitJobCommand command,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);

        var queue = command.Queue?.Trim()
            ?? throw new ArgumentException("A queue is required.", nameof(command));
        var idempotencyKey = command.IdempotencyKey?.Trim()
            ?? throw new ArgumentException("An idempotency key is required.", nameof(command));
        var payloadJson = command.Payload.GetRawText();
        var submissionHash = ComputeSubmissionHash(
            queue,
            command.Priority,
            command.MaximumAttempts,
            payloadJson);

        var candidate = Job.Create(
            idempotencyKey,
            submissionHash,
            queue,
            command.Priority,
            payloadJson,
            command.MaximumAttempts,
            timeProvider.GetUtcNow());

        var insertion = await repository.AddOrGetExistingAsync(candidate, cancellationToken);

        if (insertion.WasCreated)
        {
            return new SubmitJobResult(SubmitJobOutcome.Created, JobView.FromDomain(insertion.Job));
        }

        var outcome = CryptographicOperations.FixedTimeEquals(
            Encoding.ASCII.GetBytes(insertion.Job.SubmissionHash),
            Encoding.ASCII.GetBytes(submissionHash))
            ? SubmitJobOutcome.Existing
            : SubmitJobOutcome.IdempotencyConflict;

        return new SubmitJobResult(outcome, JobView.FromDomain(insertion.Job));
    }

    private static string ComputeSubmissionHash(
        string queue,
        int priority,
        int maximumAttempts,
        string payloadJson)
    {
        using var payloadDocument = JsonDocument.Parse(payloadJson);
        var normalizedRequest = JsonSerializer.Serialize(new
        {
            Queue = queue,
            Priority = priority,
            MaximumAttempts = maximumAttempts,
            Payload = payloadDocument.RootElement
        });

        return Convert.ToHexStringLower(
            SHA256.HashData(Encoding.UTF8.GetBytes(normalizedRequest)));
    }
}
