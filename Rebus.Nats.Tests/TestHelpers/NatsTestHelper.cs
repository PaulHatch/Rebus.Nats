using NATS.Client.Core;

namespace Rebus.Nats.Tests.TestHelpers;

public static class NatsTestHelper
{
    public static async Task<bool> IsNatsAvailable(string connectionString)
    {
        try
        {
            var opts = new NatsOpts { Url = connectionString };
            await using var connection = new NatsConnection(opts);
            await connection.ConnectAsync();
            return true;
        }
        catch
        {
            return false;
        }
    }

    public static async Task WaitForNats(string connectionString, TimeSpan timeout)
    {
        var startTime = DateTime.UtcNow;

        while (DateTime.UtcNow - startTime < timeout)
        {
            if (await IsNatsAvailable(connectionString))
            {
                return;
            }

            await Task.Delay(100);
        }

        throw new TimeoutException($"NATS did not become available within {timeout}");
    }
}
