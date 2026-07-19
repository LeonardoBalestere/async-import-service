using ImportService.Data;

namespace ImportService.Tests;

public class JobStatusEntryTests
{
    private static JobStatusEntry Entry(long expiresAtUnixSeconds) => new(
        Guid.NewGuid(), "Completed", DateTimeOffset.UtcNow, expiresAtUnixSeconds, 100, null, null);

    [Fact]
    public void Item_dentro_do_ttl_nao_esta_expirado()
    {
        var now = DateTimeOffset.UtcNow;

        Assert.False(Entry(now.AddMinutes(5).ToUnixTimeSeconds()).IsExpired(now));
    }

    [Fact]
    public void Item_com_expiresAt_no_passado_esta_expirado_mesmo_que_fisicamente_presente()
    {
        var now = DateTimeOffset.UtcNow;

        Assert.True(Entry(now.AddSeconds(-1).ToUnixTimeSeconds()).IsExpired(now));
    }
}
