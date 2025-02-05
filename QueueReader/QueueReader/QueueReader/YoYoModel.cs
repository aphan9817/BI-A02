/*
* FILE : YoYoModel.cs
* PROJECT : SENG3120 - Assignment #2
* PROGRAMMER : Anthony Phan & Jake Warywoda
* FIRST VERSION : 2025-01-31
* DESCRIPTION :
* The functions in this file are used to house the data elements of the message
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueueReader
{
    /*
    * NAME : YoYoModel
    * PURPOSE : The YoYoModel class models the data elements of a message received from a message queue.
    *           The YoYo simulator acts as a physical plant that sends messages to a queue and this program
    *           reads from the queue and processes its data.
    */
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
