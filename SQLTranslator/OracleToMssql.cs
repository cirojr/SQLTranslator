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
            _appSettings = appSettings?.CurrentValue;
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
            TranslateFileLines(file, fileLines);
        }

        private void TranslateFileLines(string file, IEnumerable<string> fileLines)
        {
            var fileLinesToWrite = new List<string>();
            var exportedRows = new List<string>();
            var fieldsArray = new List<string>();
            var valuesArray = new List<string>();
            string tableName = string.Empty;
            var rowsToExportNumber = 20000;
            var fileIndex = 0;
            var fileName = string.Empty;

            if (!fileLines.Any())
            {
                return;
            }

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
                        var replacedLine = line;

                        var textsBetweenBarresRegex = new Regex(@"' [|][|] .* [|][|] '");
                        var textsBetweenBarresMatches = textsBetweenBarresRegex.Matches(replacedLine);
                        foreach (Match text in textsBetweenBarresMatches)
                        {
                            replacedLine = replacedLine.Replace(text.Value, text.Value.Replace("' || chr(13) || '", " ", StringComparison.InvariantCulture)
                                                                                      .Replace("' || chr(9) || '", " ", StringComparison.InvariantCulture),
                                                                            StringComparison.InvariantCulture);
                        }

                        var apostropheBetweenBarresRegex = new Regex(@"[|] '[^|()]*' [|]");
                        var apostropheBetweenBarresMatches = apostropheBetweenBarresRegex.Matches(replacedLine);
                        foreach (Match text in apostropheBetweenBarresMatches)
                        {
                            replacedLine = replacedLine.Replace(text.Value, text.Value.Replace("'", string.Empty, StringComparison.InvariantCulture), StringComparison.InvariantCulture);
                        }

                        var multipleApostrophesTogetherRegex = new Regex(@".[']{3,}.");
                        var multipleApostrophesTogetherMatches = multipleApostrophesTogetherRegex.Matches(replacedLine);
                        foreach (Match text in multipleApostrophesTogetherMatches)
                        {
                            if (text.Value.EndsWith("',", StringComparison.InvariantCulture)){
                                replacedLine = replacedLine.Replace(text.Value, text.Value.Replace("'", string.Empty, StringComparison.InvariantCulture)
                                    .Replace(",", "',", StringComparison.InvariantCulture), StringComparison.InvariantCulture);
                            }
                            if (text.Value.StartsWith(",'", StringComparison.InvariantCulture)){
                                replacedLine = replacedLine.Replace(text.Value, text.Value.Replace("'", string.Empty, StringComparison.InvariantCulture)
                                    .Replace(",", ",'", StringComparison.InvariantCulture), StringComparison.InvariantCulture);
                            }
                            replacedLine = replacedLine.Replace(text.Value, text.Value.Replace("'''", "'", StringComparison.InvariantCulture), StringComparison.InvariantCulture);
                        }

                        var singleApostropheRegex = new Regex(@"([,]|[,][\s]['][\s]{0,1}|[^,][\s]|[(][']|[\w]|[^',][^',(])'([\s]['][,]|[\s][']|[^',)]|[\w]|['][,)]|[^,][\s])");
                        var singleApostropheMatches = singleApostropheRegex.Matches(replacedLine);
                        foreach(Match text in singleApostropheMatches)
                        {
                            if (text.Value.Contains("' '", StringComparison.InvariantCulture))
                            {
                                replacedLine = replacedLine.Replace(text.Value, text.Value.Replace("' ',", "',", StringComparison.InvariantCulture), StringComparison.InvariantCulture);
                                replacedLine = replacedLine.Replace(text.Value, text.Value.Replace(", ' '", ", '", StringComparison.InvariantCulture), StringComparison.InvariantCulture);
                                replacedLine = replacedLine.Replace(text.Value, text.Value.Replace("' '", string.Empty, StringComparison.InvariantCulture), StringComparison.InvariantCulture);
                                continue;
                            }
                            if (text.Value.Contains("''", StringComparison.InvariantCulture))
                            {
                                replacedLine = replacedLine.Replace(text.Value, text.Value.Replace(" '',", " ',", StringComparison.InvariantCulture), StringComparison.InvariantCulture);
                                replacedLine = replacedLine.Replace(text.Value, text.Value.Replace(", ''", ", '", StringComparison.InvariantCulture), StringComparison.InvariantCulture);
                                replacedLine = replacedLine.Replace(text.Value, text.Value.Replace(" '' ", string.Empty, StringComparison.InvariantCulture), StringComparison.InvariantCulture);
                                continue;
                            }

                            replacedLine = replacedLine.Replace(text.Value, text.Value.Replace("'", string.Empty, StringComparison.InvariantCulture), StringComparison.InvariantCulture);
                        }

                        var doubleApostrophesAfterNumberRegex = new Regex(@"\d''");
                        var doubleApostrophesAfterNumberMatches = doubleApostrophesAfterNumberRegex.Matches(replacedLine);
                        foreach (Match text in doubleApostrophesAfterNumberMatches)
                        {
                            if (text.Value.Contains("''", StringComparison.InvariantCulture))
                            {
                                replacedLine = replacedLine.Replace(text.Value, text.Value.Replace("''", string.Empty, StringComparison.InvariantCulture), StringComparison.InvariantCulture);
                            }
                        }

                        //checking if strings after nulls had lost their apostrophe
                        //var nullRegex = new Regex(@"null[,][\s][\D][\w]*");
                        //var nullMatches = nullRegex.Matches(replacedLine);

                        //foreach (Match text in nullMatches)
                        //{
                        //    if (!text.Value.Contains("to_date", StringComparison.InvariantCulture))
                        //    {
                        //        replacedLine = replacedLine.Replace(text.Value, text.Value.Replace("null, ", "null, '", StringComparison.InvariantCulture), StringComparison.InvariantCulture);
                        //    }
                        //}

                        replacedLine = replacedLine.Replace("'T'", "1", StringComparison.InvariantCulture);
                        replacedLine = replacedLine.Replace("'F'", "0", StringComparison.InvariantCulture);
                        replacedLine = replacedLine.Replace("\"", string.Empty, StringComparison.InvariantCulture);

                        //var textsBetweenApostrophesRegex = new Regex(@"[(]'[^\s]([^']+?)'[,]|\s'[^\s]([^']+?)'[)]|\s'[^\s](.+?)',|\s'([\s\S])+?'[,]");
                        var textsBetweenApostrophesRegex = new Regex(@"[(]'([^']+?)'[,]|\s'[^\s]([^']+?)'[)]|\s'[^\s](.+?)',|\s'([\s\S])+?'[,]");
                        var textsMatches = textsBetweenApostrophesRegex.Matches(replacedLine);
                        foreach (Match text in textsMatches)
                        {
                            if (text.Value.StartsWith(','))
                            {
                                replacedLine = replacedLine.Replace(text.Value.Trim('"').Trim(), $",{text.Value.Trim('"').Trim().Trim(',').Replace(',', ' ')}", StringComparison.InvariantCulture);
                            }
                            else if (text.Value.EndsWith(','))
                            {
                                replacedLine = replacedLine.Replace(text.Value.Trim('"').Trim(), $"{text.Value.Trim('"').Trim().Trim(',').Replace(',', ' ')},", StringComparison.InvariantCulture);
                            }
                        }

                        valuesArray = string.IsNullOrWhiteSpace(replacedLine) ? line.Split(',').ToList() : replacedLine.Split(',').ToList();

                        var lineData = GetRowData(fieldsArray, valuesArray);
                        if (lineData != null)
                        {
                            var stringRow = RowToString(tableName, lineData);
                            exportedRows.Add(stringRow);
                            //fileLinesToWrite.Add(stringRow);
                            
                            if (exportedRows.Count % rowsToExportNumber == 0)
                            {
                                _logger.Log(LogLevel.Information, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{file}|Traduction lignes {exportedRows.Count - rowsToExportNumber} à {exportedRows.Count}");
                                fileIndex = exportedRows.Count / rowsToExportNumber;
                                fileName = $"{fileIndex}.{Path.GetFileNameWithoutExtension(file)}{Path.GetExtension(file)}";
                                _logger.Log(LogLevel.Information, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{fileName}|Écriture de nouveau script");

                                fileLinesToWrite.Clear();
                                fileLinesToWrite.Add(GetScriptHeader());
                                fileLinesToWrite.Add(GetBeginTransactionStatement());

                                fileLinesToWrite.AddRange(exportedRows.Skip(exportedRows.Count - rowsToExportNumber).Take(rowsToExportNumber));

                                fileLinesToWrite.Add(GetEndTransactionStatement(exportedRows.Count - rowsToExportNumber, rowsToExportNumber, tableName));

                                _fileServices.WriteMSSqlFile(fileLinesToWrite, _appSettings.Paths.Output, fileName);
                            }
                        }
                    }
                }

                var previousExportedRowsCount = fileIndex * rowsToExportNumber;
                var lastTreateddRowsCount = exportedRows.Count - previousExportedRowsCount;
                _logger.Log(LogLevel.Information, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{file}|Traduction lignes {previousExportedRowsCount} à {lastTreateddRowsCount}");
                fileIndex++;
                fileName = $"{fileIndex}.{Path.GetFileNameWithoutExtension(file)}{Path.GetExtension(file)}";
                _logger.Log(LogLevel.Information, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{fileName}|Écriture de dernier script");

                fileLinesToWrite.Clear();
                fileLinesToWrite.Add(GetScriptHeader());
                fileLinesToWrite.Add(GetBeginTransactionStatement());

                fileLinesToWrite.AddRange(exportedRows.Skip(previousExportedRowsCount).Take(lastTreateddRowsCount));

                fileLinesToWrite.Add(GetEndTransactionStatement(previousExportedRowsCount, lastTreateddRowsCount, tableName));

                _fileServices.WriteMSSqlFile(fileLinesToWrite, _appSettings.Paths.Output, fileName);
            }
            catch (Exception e)
            {
                _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{e}");
                Environment.ExitCode = 1;
            }
        }

        private IDictionary<string, string> GetRowData(IList<string> fieldsArray, IList<string> valuesArray)
        {
            var exportedRow = new Dictionary<string, string>();
            var valueIndex = 0;

            for (var fieldIndex = 0; fieldIndex < fieldsArray.Count; fieldIndex++)
            {
                string trimmedFieldName;
                string trimmedValue = string.Empty;
                

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

                try
                {
                    trimmedValue = valuesArray[valueIndex].Trim().Trim(new char[] { ')', ';' });
                }                
                catch(ArgumentOutOfRangeException ex)
                {
                    _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{ex}");
                }

                if (string.IsNullOrWhiteSpace(trimmedValue))
                {
                    trimmedValue = "null";
                }

                if (trimmedValue.Substring(0, 1) == "\'")
                {
                    trimmedValue = $"'{trimmedValue.Trim('\'').Trim()}'";
                }

                // to_date function is divided in two arrays positions, so we need to jump one more index
                // for the same field position
                if (trimmedValue.Contains("to_date", StringComparison.InvariantCulture))
                {
                    var date = trimmedValue.Split('(')[1].Trim('\'');
                    var format = valuesArray[valueIndex + 1].Trim().Trim(new char[] { '\'', ')', ';' }).Replace('m', 'M').Split(' ')[0];

                    if (date.Split(' ').Length == 2)
                    {
                        format += " HH:mm:ss";
                    }

                    date = DateTime.ParseExact(date, format, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);

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
                   "-- Cet script template est protecté avec transactions: il peut être exécuté plusieurs fois sans erreurs ou inconsistence des données\n" +
                   "\n" +
                   "SET XACT_ABORT ON\n" +
                   "SET NOCOUNT ON\n";
        }

        private string GetBeginTransactionStatement()
        {
            return "BEGIN TRANSACTION\n\n" +
                   "DECLARE @RowsToBeImported INT\n" +
                   "DECLARE @ExpectedRowCount INT\n" +
                   "DECLARE @ActualRowCount INT\n";
        }

        private string GetEndTransactionStatement(int rowsInTable, int rowsCount, string tableName)
        {
            return $"\nSET @RowsToBeImported = {rowsCount}\n" +
                   $"SET @ExpectedRowCount = {rowsInTable + rowsCount}\n" +
                   $"SET @ActualRowCount = (SELECT COUNT(*) FROM {tableName})\n\n" +
                   "RAISERROR('Rows expected to be imported: %d', 0, 1, @RowsToBeImported) WITH NOWAIT\n" +
                   "RAISERROR('Expected new row count: %d', 0, 1, @ExpectedRowCount) WITH NOWAIT\n" +
                   "RAISERROR('Current new row count: %d', 0, 1, @ActualRowCount) WITH NOWAIT\n" +
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