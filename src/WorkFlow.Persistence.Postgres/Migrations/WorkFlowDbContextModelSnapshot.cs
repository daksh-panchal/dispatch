using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;

namespace WorkFlow.Persistence.Postgres.Migrations;

[DbContext(typeof(WorkFlowDbContext))]
public sealed class WorkFlowDbContextModelSnapshot : ModelSnapshot
{
    protected override void BuildModel(ModelBuilder modelBuilder)
    {
        modelBuilder
            .HasAnnotation("ProductVersion", "10.0.4")
            .HasAnnotation("Relational:MaxIdentifierLength", 63);

        modelBuilder.Entity("WorkFlow.Domain.Jobs.Job", entity =>
        {
            entity.Property<Guid>("Id")
                .ValueGeneratedNever()
                .HasColumnType("uuid")
                .HasColumnName("id");

            entity.Property<int>("AttemptCount")
                .HasColumnType("integer")
                .HasColumnName("attempt_count");

            entity.Property<DateTimeOffset>("AvailableAt")
                .HasColumnType("timestamp with time zone")
                .HasColumnName("available_at");

            entity.Property<DateTimeOffset>("CreatedAt")
                .HasColumnType("timestamp with time zone")
                .HasColumnName("created_at");

            entity.Property<string>("IdempotencyKey")
                .IsRequired()
                .HasMaxLength(200)
                .HasColumnType("character varying(200)")
                .HasColumnName("idempotency_key");

            entity.Property<int>("MaximumAttempts")
                .HasColumnType("integer")
                .HasColumnName("maximum_attempts");

            entity.Property<string>("PayloadJson")
                .IsRequired()
                .HasColumnType("jsonb")
                .HasColumnName("payload");

            entity.Property<int>("Priority")
                .HasColumnType("integer")
                .HasColumnName("priority");

            entity.Property<string>("Queue")
                .IsRequired()
                .HasMaxLength(100)
                .HasColumnType("character varying(100)")
                .HasColumnName("queue");

            entity.Property<int>("State")
                .HasMaxLength(32)
                .HasColumnType("character varying(32)")
                .HasColumnName("state");

            entity.Property<string>("SubmissionHash")
                .IsRequired()
                .HasColumnType("character(64)")
                .HasColumnName("submission_hash");

            entity.Property<DateTimeOffset>("UpdatedAt")
                .HasColumnType("timestamp with time zone")
                .HasColumnName("updated_at");

            entity.HasKey("Id")
                .HasName("pk_jobs");

            entity.HasIndex("IdempotencyKey")
                .IsUnique()
                .HasDatabaseName("ux_jobs_idempotency_key");

            entity.HasIndex("Queue", "State", "AvailableAt", "Priority")
                .HasDatabaseName("ix_jobs_queue_state_available_priority");

            entity.ToTable("jobs", table =>
            {
                table.HasCheckConstraint("ck_jobs_attempt_count", "attempt_count >= 0");
                table.HasCheckConstraint("ck_jobs_maximum_attempts", "maximum_attempts >= 1");
                table.HasCheckConstraint(
                    "ck_jobs_priority",
                    "priority >= -1000 AND priority <= 1000");
            });
        });
    }
}
