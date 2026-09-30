namespace WorkFlow.Domain.Jobs;

public enum JobState
{
    Queued,
    Leased,
    Running,
    Succeeded,
    Failed,
    Cancelled
}
