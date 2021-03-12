using System;
using System.Collections.Generic;
using System.Text;

namespace DataLakeCrawler
{
    public class CreateRequest
    {
        public string Path { get; set; }
        public int MaxDepth { get; set; }
        public int CurrentDepth { get; set; }
        public int NumberOfFDirectories { get; set; }
        public int NumberOfFiles { get; set; }
        public int NumberOfAcls { get; set; }
        public bool CreateFiles { get; set; }
        public bool CreateAcls { get; set; }
        public string DirectoryPattern { get; set; }
        public string FilePattern { get; set; }
    }
}
