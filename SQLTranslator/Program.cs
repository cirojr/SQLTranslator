using System;
using System.IO;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NLog.Extensions.Logging;

/*************************************************************************************************************************************************************
* Date              Author                      Description
* ============================================================================================================================================================
* 2020-01-16        Ciro Antunes                Entry point for 'SQLTranslator' program. Checks and loads configuration files and service classes, then
*                                               run 'SQLTranslator' main service
/*************************************************************************************************************************************************************/

namespace SQLTranslator
{
    public static class Program
    {
        public static void Main()
        {
            var services = new ServiceCollection();

            ConfigureServices(services);

            var serviceProvider = services.BuildServiceProvider();

            serviceProvider.GetService<OracleToMssql>().Run();
        }

        private static void ConfigureServices(IServiceCollection services)
        {
            var defaultJsonFile = "appsettings.json";

            //Gets appsettings configurations
            var configurationBuilder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile(defaultJsonFile, optional: false, reloadOnChange: true)
                .Build();

            services.AddOptions<AppSettingsModel>()
                .Bind(configurationBuilder);

            //Adds logging
            services.AddLogging(loggingBuilder =>
            {
                loggingBuilder.ClearProviders();
                loggingBuilder.AddNLog(new NLogLoggingConfiguration(configurationBuilder.GetSection("nlog")));
            });

            //Adds custom services
            services.AddTransient<IFileServices, FileServices>();

            //Adds main program
            services.AddTransient<OracleToMssql>();
        }

        /*
        private static void ValidateSettingsExist(string environmentName, string defaultJsonFile, string environmentJsonFile)
        {
            var logger = new FallBackLogger();
            var defaultJsonFilePath = Path.Combine(Directory.GetCurrentDirectory(), defaultJsonFile);
            var environmentJsonFilePath = Path.Combine(Directory.GetCurrentDirectory(), environmentJsonFile);

            if (string.IsNullOrWhiteSpace(environmentName))
            {
                logger.Log("Variable d'environnement 'ICoCEnvironment' introuvable");
                Environment.Exit(1);
            }

            if (environmentName != "development" &&
                environmentName != "acceptance" &&
                environmentName != "production")
            {
                logger.Log("Valeur de variable de environnement 'ICoCEnvironment' inconnue");
                Environment.Exit(1);
            }

            if (!File.Exists(defaultJsonFilePath) &&
                !File.Exists(environmentJsonFilePath))
            {
                logger.Log($"Fichiers de configuration json: {defaultJsonFile}, {environmentJsonFile} introuvables dans le repertoire 'Settings'");
                Environment.Exit(1);
            }
        }

        private static void ValidateSettingsModel(IOptionsMonitor<AppSettingsModel> options)
        {
            try
            {
                var currentOptions = options.CurrentValue;
            }
            catch (Exception e)
            {
                var logger = new FallBackLogger();
                logger.Log($"Validation de configurations échoué|{e}");
                Environment.Exit(1);
            }
        }*/
    }
}