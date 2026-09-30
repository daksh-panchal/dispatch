using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using WorkFlow.IntegrationTests.Infrastructure;
using Xunit;

namespace WorkFlow.IntegrationTests.Jobs;

public sealed class JobEndpointsTests(WorkFlowApiFixture fixture)
    : IClassFixture<WorkFlowApiFixture>
{
    [Fact]
    public async Task SubmittedJobCanBeRetrieved()
    {
        var idempotencyKey = $"integration-{Guid.NewGuid()}";
        using var submission = CreateSubmission(idempotencyKey);

        using var submitResponse = await fixture.Client.SendAsync(submission);

        Assert.Equal(HttpStatusCode.Created, submitResponse.StatusCode);

        using var submittedJson = JsonDocument.Parse(
            await submitResponse.Content.ReadAsStringAsync());
        var jobId = submittedJson.RootElement.GetProperty("id").GetGuid();

        using var getResponse = await fixture.Client.GetAsync($"/api/v1/jobs/{jobId}");

        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);

        using var retrievedJson = JsonDocument.Parse(
            await getResponse.Content.ReadAsStringAsync());
        Assert.Equal(jobId, retrievedJson.RootElement.GetProperty("id").GetGuid());
        Assert.Equal("queued", retrievedJson.RootElement.GetProperty("state").GetString());
    }

    [Fact]
    public async Task RepeatingAnIdempotentSubmissionReturnsTheSameJob()
    {
        var idempotencyKey = $"integration-{Guid.NewGuid()}";

        using var firstRequest = CreateSubmission(idempotencyKey);
        using var firstResponse = await fixture.Client.SendAsync(firstRequest);
        using var firstJson = JsonDocument.Parse(
            await firstResponse.Content.ReadAsStringAsync());

        using var secondRequest = CreateSubmission(idempotencyKey);
        using var secondResponse = await fixture.Client.SendAsync(secondRequest);
        using var secondJson = JsonDocument.Parse(
            await secondResponse.Content.ReadAsStringAsync());

        Assert.Equal(HttpStatusCode.Created, firstResponse.StatusCode);
        Assert.Equal(HttpStatusCode.OK, secondResponse.StatusCode);
        Assert.Equal(
            firstJson.RootElement.GetProperty("id").GetGuid(),
            secondJson.RootElement.GetProperty("id").GetGuid());
    }

    private static HttpRequestMessage CreateSubmission(string idempotencyKey)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/api/v1/jobs")
        {
            Content = JsonContent.Create(new
            {
                queue = "default",
                priority = 0,
                maximumAttempts = 3,
                payload = new
                {
                    command = "example"
                }
            })
        };

        request.Headers.Add("Idempotency-Key", idempotencyKey);
        return request;
    }
}
