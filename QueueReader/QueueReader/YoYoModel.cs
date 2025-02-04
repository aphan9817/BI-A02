using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueueReader
{
    public class YoYoModel
    {
        public string WorkArea { get; set; }
        public Guid SerialNumber { get; set;}
        public string LineNumber { get; set; }
        public string State { get; set; }
        public string Reason { get; set; }
        public DateTime TimeStamp { get; set; }
        public int ProductID { get; set; }
    }
}
