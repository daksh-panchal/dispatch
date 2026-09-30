using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace WorkFlow.Persistence.Postgres.Migrations;

[DbContext(typeof(WorkFlowDbContext))]
[Migration("20260926000000_InitialJobs")]
public sealed class InitialJobs : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "jobs",
            columns: table => new
            {
                id = table.Column<Guid>(type: "uuid", nullable: false),
                idempotency_key = table.Column<string>(
                    type: "character varying(200)",
                    maxLength: 200,
                    nullable: false),
                submission_hash = table.Column<string>(
                    type: "character(64)",
                    nullable: false),
                queue = table.Column<string>(
                    type: "character varying(100)",
                    maxLength: 100,
                    nullable: false),
                priority = table.Column<int>(type: "integer", nullable: false),
                payload = table.Column<string>(type: "jsonb", nullable: false),
                state = table.Column<string>(
                    type: "character varying(32)",
                    maxLength: 32,
                    nullable: false),
                attempt_count = table.Column<int>(type: "integer", nullable: false),
                maximum_attempts = table.Column<int>(type: "integer", nullable: false),
                available_at = table.Column<DateTimeOffset>(
                    type: "timestamp with time zone",
                    nullable: false),
                created_at = table.Column<DateTimeOffset>(
                    type: "timestamp with time zone",
                    nullable: false),
                updated_at = table.Column<DateTimeOffset>(
                    type: "timestamp with time zone",
                    nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("pk_jobs", x => x.id);
                table.CheckConstraint("ck_jobs_attempt_count", "attempt_count >= 0");
                table.CheckConstraint("ck_jobs_maximum_attempts", "maximum_attempts >= 1");
                table.CheckConstraint(
                    "ck_jobs_priority",
                    "priority >= -1000 AND priority <= 1000");
            });

        migrationBuilder.CreateIndex(
            name: "ix_jobs_queue_state_available_priority",
            table: "jobs",
            columns: new[] { "queue", "state", "available_at", "priority" });

        migrationBuilder.CreateIndex(
            name: "ux_jobs_idempotency_key",
            table: "jobs",
            column: "idempotency_key",
            unique: true);
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(name: "jobs");
    }
}
