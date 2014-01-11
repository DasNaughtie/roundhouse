using System;

namespace roundhouse.infrastructure
{
    public delegate void CreateScriptHandler(object sender, CreateScriptEventArgs e);

    public class CreateScriptEventArgs : EventArgs
    {
        public CreateScriptEventArgs(string fileName, string sqlScript, bool beforeUpScripts)
        {
            FileName       = fileName;
            SqlScript      = sqlScript;
            BeforeUpScript = beforeUpScripts;
        }
        public string FileName { get; set; }

        public string SqlScript { get; set; }

        public bool BeforeUpScript { get; set; }
    }
}
