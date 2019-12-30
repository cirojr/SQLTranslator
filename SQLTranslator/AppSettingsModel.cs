namespace SQLTranslator
{
    public class AppSettingsModel
    {
        public Paths Paths { get; set; }
    }
    public class Paths
    {
        public string Input { get; set; }

        public string InProgress { get; set; }

        public string Error { get; set; }

        public string Output { get; set; }
    }
}