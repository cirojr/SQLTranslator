using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace SQLTranslator
{
    public class OracleToMssql
    {
        private readonly ILogger _logger;
        private readonly AppSettingsModel _appSettings;
        private readonly IFileServices _fileServices;

        public OracleToMssql(ILogger<OracleToMssql> logger,
                             IOptionsMonitor<AppSettingsModel> appSettings,
                             IFileServices fileServices)
        {
            _logger = logger;
            _appSettings = appSettings.CurrentValue;
            _fileServices = fileServices;
        }
        public void Run()
        {
            _logger.Log(LogLevel.Information, "Début d'éxecution");

            _fileServices.CheckWatchPath(_appSettings.Paths.Input, "*.sql");
            var filePathList = _fileServices.GetFilePathList();

            Parallel.ForEach(filePathList, (file) => ReadFile(file));

            _logger.Log(LogLevel.Information, "Fin d'éxecution");
        }

        private void ReadFile(string file)
        {
            _logger.Log(LogLevel.Information, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{file}|Lecture du fichier");
            var fileLines = _fileServices.ReadFile(file);
            _logger.Log(LogLevel.Information, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{file}|Traduction des lignes");
            var sqlLines = TranslateFileLines(fileLines);
            _logger.Log(LogLevel.Information, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{file}|Écriture de nouveau script");
            _fileServices.WriteMSSqlFile(sqlLines, _appSettings.Paths.Output, Path.GetFileName(file));
        }

        private IEnumerable<string> TranslateFileLines(IEnumerable<string> fileLines)
        {
            var fileLinesToWrite = new List<string>();
            var exportedRows = new List<string>();
            var fieldsArray = new List<string>();
            var valuesArray = new List<string>();
            string tableName = string.Empty;

            if (!fileLines.Any())
            {
                return null;
            }

            fileLinesToWrite.Add(GetScriptHeader());
            fileLinesToWrite.Add(GetBeginTransactionStatement());

            try
            {
                foreach (var line in fileLines)
                {
                    var lineType = line.Substring(0, 6);

                    if (string.IsNullOrWhiteSpace(line) || (lineType != "insert" && lineType != "values"))
                    {
                        continue;
                    }

                    if (lineType == "insert")
                    {
                        fieldsArray = line.Split(',').ToList();
                        tableName = fieldsArray[0].Split(' ')[2].Split('.')[1];
                    }
                    else if (lineType == "values")
                    {
                        var regex = new Regex(@"'([^']+)'");
                        var matches = regex.Matches(line);
                        var replacedLine = string.Empty;

                        foreach (Match text in matches)
                        {
                            if (text.Value.Contains(','))
                            {
                                replacedLine = line.Replace(text.Value.Trim('"'), text.Value.Trim('"').Replace(',', ' '));
                            }
                        }

                        valuesArray = string.IsNullOrWhiteSpace(replacedLine) ? line.Split(',').ToList() : replacedLine.Split(',').ToList();

                        var lineData = GetRowData(fieldsArray, valuesArray);
                        if (lineData != null)
                        {
                            var stringRow = RowToString(tableName, lineData);
                            exportedRows.Add(stringRow);
                            fileLinesToWrite.Add(stringRow);

                            if (exportedRows.Count % 100000 == 0)
                            {
                                _logger.Log(LogLevel.Information, $"Thread: {Thread.CurrentThread.ManagedThreadId}|Ligne {exportedRows.Count} exporté");
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{e}");
                Environment.ExitCode = 1;
                //IsAnyLineParseError = true;
            }

            fileLinesToWrite.Add(GetEndTransactionStatement(exportedRows.Count, tableName));

            return fileLinesToWrite;
        }

        private IDictionary<string, string> GetRowData(IList<string> fieldsArray, IList<string> valuesArray)
        {
            var exportedRow = new Dictionary<string, string>();
            var valueIndex = 0;

            for (var fieldIndex = 0; fieldIndex < fieldsArray.Count; fieldIndex++)
            {
                string trimmedFieldName;
                string trimmedValue;
                

                if (fieldIndex == 0)
                {
                    trimmedFieldName = fieldsArray[fieldIndex].Split('(')[1];
                    trimmedValue = valuesArray[valueIndex].Split('(')[1].Trim();

                    if(trimmedValue.Substring(0, 1) == "\'")
                    {
                        trimmedValue = $"'{trimmedValue.Trim('\'').Trim()}'";
                    }

                    exportedRow.Add(trimmedFieldName, trimmedValue);
                    valueIndex++;
                    continue;
                }

                trimmedFieldName = fieldsArray[fieldIndex].Trim().Trim(new char[]{'\'', ')', ';'}).Trim();
                trimmedValue = valuesArray[valueIndex].Trim().Trim(new char[] {')', ';'});

                if (trimmedValue.Substring(0, 1) == "\'")
                {
                    trimmedValue = $"'{trimmedValue.Trim('\'').Trim()}'";
                }

                // to_date function is divided in two arrays positions, so we need to jump one more index
                // for the same field position
                if (trimmedValue.Contains("to_date"))
                {
                    var date = trimmedValue.Split('(')[1].Trim('\'');
                    var format = valuesArray[valueIndex + 1].Trim().Trim(new char[] { '\'', ')', ';' }).Replace('m', 'M');

                    date = DateTime.ParseExact(date, format, CultureInfo.InvariantCulture).ToString();

                    trimmedValue = $"CONVERT(DATETIME, '{date}')";

                    valueIndex++;
                }

                exportedRow.Add(trimmedFieldName, trimmedValue);
                valueIndex++;
            }

            return exportedRow;
        }

        private string GetScriptHeader()
        {
            return $"-- Script importation des lignes\n" +
                   "-- Cet script template est protecté avec transactions: il peut être exécuté plousiers fois sans erreurs ou inconsistence des données\n" +
                   "\n" +
                   "SET XACT_ABORT ON\n" +
                   "SET NOCOUNT ON\n" +
                   "\n";
        }

        private string GetBeginTransactionStatement()
        {
            return "BEGIN TRANSACTION\n\n" +
                   "DECLARE @ExpectedRowCount INT\n" +
                   "DECLARE @ActualRowCount INT\n";
        }

        private string GetEndTransactionStatement(int rowsCount, string tableName)
        {
            return $"\nSET @ExpectedRowCount = {rowsCount}\n" +
                   $"SET @ActualRowCount = (SELECT COUNT(*) FROM {tableName})\n\n" +
                   "RAISERROR('Expected row count: %d', 0, 1, @ExpectedRowCount) WITH NOWAIT\n" +
                   "RAISERROR('Current row count: %d', 0, 1, @ActualRowCount) WITH NOWAIT\n" +
                   "IF @ExpectedRowCount <> @ActualRowCount\n" +
                   "BEGIN\n" +
                   "    RAISERROR('Row count doesn''t match. Rolling back transaction.', 0, 1) WITH NOWAIT\n" +
                   "    ROLLBACK TRANSACTION\n" +
                   "END\n" +
                   "ELSE\n" +
                   "BEGIN\n" +
                   "    RAISERROR('Row count match. Committing transaction.', 0, 1) WITH NOWAIT\n" +
                   "    COMMIT TRANSACTION\n" +
                   "END";
        }

        private string RowToString(string tableName, IDictionary<string, string> row)
        {
            var columns = string.Join(", ", row.Keys);
            var values = string.Join(", ", row.Values);

            return $"INSERT INTO {tableName} ({columns}) VALUES({values});";
        }
    }
}