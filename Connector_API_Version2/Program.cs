using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MysqlEfCoreDemo
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();   // optional
                logging.AddConsole();       // ?? Enable Console.WriteLine logs
                logging.AddDebug();         // optional but helpful
            })
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.ConfigureKestrel(options =>
                    {
                        // Set the maximum request body size (in bytes)
                         options.Limits.MaxRequestBodySize = 104857600; // 100 MB
                        // options.Limits.MaxRequestBodySize = 10737418240; // 10 GB
                    });
                    webBuilder.UseStartup<Startup>();
                });
    }

}