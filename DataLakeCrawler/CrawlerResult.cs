using Azure.Storage.Files.DataLake.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataLakeCrawler
{


    public class CrawlerResult
    {
        public CrawlerResult()
        {
            this.ACLs = new List<PathAccessControlItem>();
            this.Files = new List<CrawlerFile>();
        }

        public string Path { get; set; }
        public bool IsDirectory { get; set; }

        public List<PathAccessControlItem> ACLs { get; set; }
        public List<CrawlerFile> Files { get; set; }
    }

    public class CrawlerFile
    {

        public CrawlerFile()
        {
            this.ACLs = new List<PathAccessControlItem>();
        }

        public string Name { get; set; }
        public List<PathAccessControlItem> ACLs { get; set; }
    }
}
