using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace SQLTranslator
{
    public class FileServices : IFileServices
    {
        private readonly ILogger _logger;

        private string UniqueSubFolderName { get; set; }

        private IList<string> FilePathList { get; set; }

        private IDictionary<int, List<string>> DocumentsToSend { get; set; }

        public FileServices(ILogger<FileServices> logger)
        {
            _logger = logger;
            FilePathList = new List<string>();
            DocumentsToSend = new Dictionary<int, List<string>>();
        }

        public string GetFileType(string filePath)
        {
            return Path.GetFileName(filePath).Substring(0, 2);
        }

        public void CheckWatchPath(string watchPath, string searchPattern)
        {
            if (string.IsNullOrEmpty(watchPath))
            {
                _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|Variable de dossier de lecture des fichiers texte sans valeur");
                Environment.Exit(1);
            }

            if (!Directory.Exists(watchPath))
            {
                _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|Dossier de lecture des fichiers texte introuvable: {watchPath}");
                Environment.Exit(1);
            }

            if (!Directory.EnumerateFiles(watchPath).Any())
            {
                _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|Dossier de lecture des fichiers texte vide: {watchPath}");
                Environment.Exit(2);
            }

            if (string.IsNullOrEmpty(searchPattern))
            {
                _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|Variable de patron de recherche sans valeur");
                Environment.Exit(1);
            }

            _logger.Log(LogLevel.Information, $"Répertoire de recherche: {watchPath}");
            _logger.Log(LogLevel.Information, $"Patron de recherche: {searchPattern}");

            FilePathList = Directory.EnumerateFiles(watchPath, searchPattern, SearchOption.TopDirectoryOnly).ToList();

            _logger.Log(LogLevel.Information, $"Fichiers trouvés: {FilePathList.Count}");
        }

        public void TransferFiles(string destinationPath, bool createUniqueSubFolder)
        {
            if (string.IsNullOrEmpty(destinationPath))
            {
                _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|Répertoire de destination sans valeur");
                Environment.ExitCode = 1;
            }

            var isAnyFileTransfered = false;
            var fileProcessPathList = new List<string>();

            if (createUniqueSubFolder)
            {
                destinationPath = Path.Combine(destinationPath, GenerateUniqueSubFolderName());
            }

            try
            {
                Directory.CreateDirectory(destinationPath);

                foreach (var filePath in FilePathList)
                {
                    var destinationFilePath = Path.Combine(destinationPath, Path.GetFileName(filePath));
                    if (File.Exists(destinationFilePath) || filePath.Equals(destinationFilePath))
                    {
                        _logger.Log(LogLevel.Information, $"Thread: {Thread.CurrentThread.ManagedThreadId}|Fichier {Path.GetFileName(filePath)} déjà existant dans {destinationPath}");
                        continue;
                    }

                    File.Move(filePath, destinationFilePath);
                    fileProcessPathList.Add(destinationFilePath);
                    isAnyFileTransfered = true;
                }

                if (isAnyFileTransfered)
                {
                    FilePathList = fileProcessPathList;
                    _logger.Log(LogLevel.Information, $"Fichiers transférés à {destinationPath}");
                }
            }
            catch (Exception e)
            {
                _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{e}");
                Environment.ExitCode = 1;
            }
        }

        public void TransferFile(string fileToTransferPath, string destinationPath)
        {
            if (string.IsNullOrEmpty(destinationPath))
            {
                _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|Répertoire de destination sans valeur");
                Environment.ExitCode = 1;
            }

            try
            {
                Directory.CreateDirectory(destinationPath);

                FilePathList.Remove(fileToTransferPath);

                var destinationFilePath = Path.Combine(destinationPath, Path.GetFileName(fileToTransferPath));

                if (File.Exists(destinationFilePath))
                {
                    _logger.Log(LogLevel.Information, $"Thread: {Thread.CurrentThread.ManagedThreadId}|Fichier {Path.GetFileName(fileToTransferPath)} déjà existant dans {destinationPath}");
                    return;
                }

                File.Move(fileToTransferPath, Path.Combine(destinationPath, Path.GetFileName(fileToTransferPath)));

                _logger.Log(LogLevel.Information, $"Thread: {Thread.CurrentThread.ManagedThreadId}|Fichier transféré à {destinationPath}");
            }
            catch (Exception e)
            {
                _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{e}");
                Environment.ExitCode = 1;
            }
        }

        private string GenerateUniqueSubFolderName()
        {
            var processId = Process.GetCurrentProcess().Id.ToString();
            UniqueSubFolderName = $"{DateTime.Now.ToString("yyyyMMdd")}.ProcessId{processId}";
            return UniqueSubFolderName;
        }

        public void RemoveUniqueSubFolder(string path)
        {
            try
            {
                var uniqueSubFolderPath = Path.Combine(path, UniqueSubFolderName);
                Directory.Delete(uniqueSubFolderPath, recursive: true);
            }
            catch (Exception e)
            {
                _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{e}");
                Environment.ExitCode = 1;
            }
        }

        public IList<string> GetFilePathList()
        {
            return FilePathList;
        }

        public void AddToFilePathList(string filePath)
        {
            FilePathList.Add(filePath);
        }

        public IEnumerable<string> ReadFile(string filePath)
        {
            // Register encodings for console
            // https://github.com/dotnet/roslyn/issues/10785
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

            try
            {
                //Windows-1252 code page ("ANSI")
                return File.ReadLines(filePath, Encoding.GetEncoding(1252));
            }
            catch (Exception e)
            {
                _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{e}");
                Environment.ExitCode = 1;
            }

            return null;
        }

        public void WriteMSSqlFile(IEnumerable<string> exportedRows, string fileWritePath, string fileName)
        {
            if (string.IsNullOrEmpty(fileWritePath))
            {
                _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|Répertoire de destination sans valeur");
                Environment.ExitCode = 1;
            }

            try
            {
                Directory.CreateDirectory(fileWritePath);

                var fileFullPath = Path.Combine(fileWritePath, fileName);

                var sqlFile = new StreamWriter(fileFullPath, append: false);

                foreach (var row in exportedRows)
                {
                    sqlFile.WriteLine(row);
                }

                sqlFile.Close();
            }
            catch (Exception e)
            {
                _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{e}");
                Environment.ExitCode = 1;
            }
        }

        /*
        public IDocumentType GetParsedFileObject(string filePath)
        {
            var fileType = GetFileType(filePath);

            var fileTypeInstances = new Dictionary<string, IDocumentType>
            {
                { "OC", new OrderConfirmationModel(_logger) },
                { "SA", new ServiceAgreementModel(_logger) }
            };

            try
            {
                var typeInstance = fileTypeInstances[fileType];
                typeInstance.ParseTextFile(filePath);

                return typeInstance.Validate() ? typeInstance : null;
            }
            catch (Exception e)
            {
                _logger.Log(LogLevel.Error, $"Thread: {Thread.CurrentThread.ManagedThreadId}|{e}");
                Environment.ExitCode = 1;
                return null;
            }
        }*/

        public string UpdateFileNameToSSRSOutputPath(string fileName, string ssrsOutputPath)
        {
            return Path.Combine(ssrsOutputPath, $"{Path.GetFileNameWithoutExtension(fileName)}.mhtml");
        }

        public string GetPDFFileFromDocument(Dictionary<string, string> attachments, string company, int langID)
        {
            var pdfFilePath = string.Empty;
            attachments.TryGetValue($"{company}_{langID}", out pdfFilePath);
            return pdfFilePath;
        }

        public bool IsFilePresent(string filePath)
        {
            return File.Exists(filePath);
        }

        public void RemoveFile(string filePath)
        {
            File.Delete(filePath);
        }
    }
}
