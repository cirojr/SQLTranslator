using System;
using System.Collections.Generic;
using System.Text;

namespace SQLTranslator
{
    public interface IFileServices
    {
        void CheckWatchPath(string watchPath, string searchPattern);

        void TransferFiles(string destinationPath, bool createUniqueSubFolder);

        void TransferFile(string fileToTransferPath, string destinationPath);

        IList<string> GetFilePathList();

        void AddToFilePathList(string filePath);

        string GetFileType(string filePath);

        IEnumerable<string> ReadFile(string filePath);

        void WriteMSSqlFile(IEnumerable<string> exportedRows, string fileWritePath, string fileName);

        // GetParsedFileObject(string filePath);

        void RemoveUniqueSubFolder(string path);

        string UpdateFileNameToSSRSOutputPath(string fileName, string ssrsOutputPath);

        string GetPDFFileFromDocument(Dictionary<string, string> attachments, string company, int langID);

        bool IsFilePresent(string filePath);

        void RemoveFile(string filePath);
    }
}