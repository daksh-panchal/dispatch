using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using WorkFlow.Domain.Jobs;

namespace WorkFlow.Persistence.Postgres.Configurations;

internal sealed class JobConfiguration : IEntityTypeConfiguration<Job>
{
    public void Configure(EntityTypeBuilder<Job> builder)
    {
        builder.ToTable("jobs", tableBuilder =>
        {
            tableBuilder.HasCheckConstraint(
                "ck_jobs_attempt_count",
                "attempt_count >= 0");
            tableBuilder.HasCheckConstraint(
                "ck_jobs_maximum_attempts",
                "maximum_attempts >= 1");
            tableBuilder.HasCheckConstraint(
                "ck_jobs_priority",
                "priority >= -1000 AND priority <= 1000");
        });

        builder.HasKey(job => job.Id)
            .HasName("pk_jobs");

        builder.Property(job => job.Id)
            .HasColumnName("id")
            .ValueGeneratedNever();

        builder.Property(job => job.IdempotencyKey)
            .HasColumnName("idempotency_key")
            .HasMaxLength(Job.MaximumIdempotencyKeyLength)
            .IsRequired();

        builder.Property(job => job.SubmissionHash)
            .HasColumnName("submission_hash")
            .HasColumnType("character(64)")
            .IsRequired();

        builder.Property(job => job.Queue)
            .HasColumnName("queue")
            .HasMaxLength(Job.MaximumQueueNameLength)
            .IsRequired();

        builder.Property(job => job.Priority)
            .HasColumnName("priority");

        builder.Property(job => job.PayloadJson)
            .HasColumnName("payload")
            .HasColumnType("jsonb")
            .IsRequired();

        builder.Property(job => job.State)
            .HasColumnName("state")
            .HasConversion<string>()
            .HasMaxLength(32)
            .IsRequired();

        builder.Property(job => job.AttemptCount)
            .HasColumnName("attempt_count");

        builder.Property(job => job.MaximumAttempts)
            .HasColumnName("maximum_attempts");

        builder.Property(job => job.AvailableAt)
            .HasColumnName("available_at");

        builder.Property(job => job.CreatedAt)
            .HasColumnName("created_at");

        builder.Property(job => job.UpdatedAt)
            .HasColumnName("updated_at");

        builder.HasIndex(job => job.IdempotencyKey)
            .IsUnique()
            .HasDatabaseName("ux_jobs_idempotency_key");

        builder.HasIndex(job => new
            {
                job.Queue,
                job.State,
                job.AvailableAt,
                job.Priority
            })
            .HasDatabaseName("ix_jobs_queue_state_available_priority");
    }
}
