using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

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
            _logger.Log(LogLevel.Information, $"Thread: {Thread.CurrentThread.ManagedThreadId}|Début d'éxecution");

            _fileServices.CheckWatchPath(_appSettings.Paths.Input, "*.sql");
            var filePathList = _fileServices.GetFilePathList();

            foreach(var file in filePathList)
            {
                _logger.Log(LogLevel.Information, $"Thread: {Thread.CurrentThread.ManagedThreadId}|Lecture du fichier {file}");
                var fileLines = _fileServices.ReadFile(file);
                var sqlLines = Translate(fileLines);
                _fileServices.WriteMSSqlFile(sqlLines, _appSettings.Paths.Output, Path.GetFileName(file));
            }
        }

        public IEnumerable<string> Translate(IEnumerable<string> fileLines)
        {
            var exportedTable = new Dictionary<string, IEnumerable<IDictionary<string, string>>>();
            var exportedRows = new List<string>();
            var fieldsArray = new List<string>();
            var valuesArray = new List<string>();
            string tableName = string.Empty;

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
                        tableName = fieldsArray[0].Split(' ')[2];
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

                        valuesArray = matches.Any() ? replacedLine.Split(',').ToList() : line.Split(',').ToList();

                        var lineData = GetRowData(fieldsArray, valuesArray);
                        if (lineData != null)
                        {
                            exportedRows.Add(RowToString(tableName, lineData));
                            //exportedRows.Add(lineData);
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

            return exportedRows;
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

                    trimmedValue = $"CONVERT(DATETIME, '{date}', 5)";

                    valueIndex++;
                }

                exportedRow.Add(trimmedFieldName, trimmedValue);
                valueIndex++;
            }

            return exportedRow;
        }

        private string RowToString(string tableName, IDictionary<string, string> row)
        {
            var columns = string.Join(", ", row.Keys);
            var values = string.Join(", ", row.Values);

            return $"INSERT INTO {tableName} ({columns}) VALUES({values});";
        }
    }
}
