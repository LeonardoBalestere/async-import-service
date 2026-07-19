using Microsoft.EntityFrameworkCore;

namespace ImportService.Data;

public class ImportDbContext(DbContextOptions<ImportDbContext> options) : DbContext(options)
{
    public DbSet<ImportJob> ImportJobs => Set<ImportJob>();
    public DbSet<ImportedTransaction> ImportedTransactions => Set<ImportedTransaction>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ImportJob>(job =>
        {
            job.HasIndex(j => j.FileSha256).IsUnique();
            job.Property(j => j.FileName).HasMaxLength(260);
            job.Property(j => j.FileSha256).HasMaxLength(64);
            job.Property(j => j.Bucket).HasMaxLength(63);
            job.Property(j => j.ObjectKey).HasMaxLength(1024);
            // String no banco: legível num SELECT de troubleshooting, e imune a
            // reordenação do enum.
            job.Property(j => j.Status).HasConversion<string>().HasMaxLength(20);
        });

        modelBuilder.Entity<ImportedTransaction>(tx =>
        {
            tx.HasIndex(t => new { t.JobId, t.RowNumber }).IsUnique();
            tx.Property(t => t.Account).HasMaxLength(50);
            tx.Property(t => t.Description).HasMaxLength(500);
            tx.Property(t => t.Amount).HasPrecision(18, 2);
            tx.HasOne<ImportJob>().WithMany().HasForeignKey(t => t.JobId);
        });
    }
}
