/*
* FILE : program.cs
* PROJECT : SENG3120 - Assignment #2
* PROGRAMMER : Anthony Phan & Jake Warywoda
* FIRST VERSION : 2025-01-31
* DESCRIPTION :
* The functions in this file are used to read messages from the Microsoft Message Queue Server and stores
* the data to the database as fast and efficiently as possible
*/

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using MSMQ.Messaging;

namespace QueueReader
{
    class Program
    {
        private static Boolean bRead = false;
        
        // database connection string
        private static readonly string connectionString = "Server=184.146.173.161\\SQLExpress,1300;Database=YoYoDB;User Id=Anthony;Password=YoYo1300;Encrypt=True;TrustServerCertificate=True;";
        
        // -- Jake's database connection string --
        // string connectionString = "Server=localhost;Database=YoYoDB;User Id=JakeDesktop;" +
        //"Password=YoYo1300;TrustServerCertificate=True;";
        // string connectionString = "Server=localhost;Database=YoYoDB;Integrated Security=True;";

        // list to act as a buffer for message queue
        private static List<YoYoModel> messages = new List<YoYoModel>();

        // batch size for number of messages for bulk inserting
        private const int BatchSize = 50;

        // FUNCTION    : Main
        // DESCRIPTION : Asynchronously reads messages from the Microsoft Message Queue Server, parses the data,
        //               and bulk stores it to the database.
        // PARAMETERS  : string[] args
        // RETURNS     : Task
        static async Task Main(string[] args)
        {

            Console.WriteLine("Queue reader starting");

            // get the Message Queue Server Path
            string msmqPath = GetMSMQPath();

            try
            {
                using (var msmq = new MessageQueue(msmqPath))
                {
                    // set the 
                    msmq.Formatter = new ActiveXMessageFormatter();
                    Console.WriteLine($"Connected to queue: {msmqPath}");

                    Console.WriteLine("press 's' to start, 'q' to quit");

                    while (true)
                    {
                        // get input key from user
                        var input = Console.ReadKey(true).KeyChar;

                        // start reading from message queue if 's' is pressed
                        if (input == 's')
                        {
                            if (!bRead)
                            {
                                Console.WriteLine("Reading messages..");
                                bRead = true;

                                // start reading messages asynchronously
                                await ReadMSMQ(msmq);
                            }
                            else
                            {
                                Console.WriteLine($"reading {input}");
                            }
                        }
                        // quit program if 'q' is pressed
                        else if (input == 'q')
                        {
                            Console.WriteLine("Message Queue reader stopping..");
                            break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception: {ex.Message}");
            }
        }

        // FUNCTION    : ReadMSMQ
        // DESCRIPTION : Asynchronously reads messages from the Microsoft Message Queue Server, parses the data,
        //               and bulk stores it to the database.
        // PARAMETERS  : MessageQueue msmq
        // RETURNS     : Task
        private static async Task ReadMSMQ(MessageQueue msmq)
        {
            // list to keep track of tasks
            List<Task> tasks = new List<Task>();

            while (bRead)
            {
                try
                {
                    // asynchronously read messages from the MSMQ
                    var messageTask = Task.Factory.FromAsync(msmq.BeginReceive(),msmq.EndReceive)
                        .ContinueWith(async t =>
                        {
                            // read message from MSMQ
                            var message = t.Result;
                            // read the message body
                            string messageBody = message.Body.ToString();
                            Console.WriteLine(messageBody);

                            // parse the message into an object
                            var yoyoModel = ParseMessage(messageBody);
                            // add the message to the messages buffer
                            messages.Add(yoyoModel);

                            // check if the message count exceeds the batch size for bulk inserting
                            if (messages.Count >= BatchSize)
                            {
                                // bulk insert data into the database
                                bool success = await BulkStoreToDatabase(messages);

                                if (!success)
                                {
                                    Console.WriteLine("failed to store to database");

                                    // stop reading if writing to the database fails
                                    bRead = false;
                                }
                                // clear the buffer after inserting to the database
                                messages.Clear();
                            }
                        });

                    // add the message reading task to the list of tasks
                    tasks.Add(messageTask);

                    // limit the tasks to 10 just in case
                    if ( tasks.Count >= 10)
                    {
                        // wait for any task to complete
                        await Task.WhenAny(tasks);

                        // remove alll tasks from the list once completed
                        tasks.RemoveAll(t => t.IsCompleted);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unhandled Exception: {ex.Message}");
                }
            }

            // check when all tasks are completed
            await Task.WhenAll(tasks);

            // insert any leftover messages
            if (messages.Count > 0)
            {
                await BulkStoreToDatabase(messages);
                Console.WriteLine("Remaining messages have been inserted");
            }
        }

        // FUNCTION    : ParseMessage
        // DESCRIPTION : Parse the message string into parts and store data elements into class
        // PARAMETERS  : string message
        // RETURNS     : YoYoModel class
        private static YoYoModel ParseMessage(string message)
        {
            // split the message wherever there is a ','
            var parts = message.Split(',');

            return new YoYoModel
            {
                WorkArea = parts[0],
                SerialNumber = Guid.Parse(parts[1]),
                LineNumber = parts[2],
                State = parts[3],
                Reason = parts[4],
                TimeStamp = DateTime.Parse(parts[5]),
                ProductID = int.Parse(parts[6])
            };
        }

        // FUNCTION    : BulkStoreToDatabase
        // DESCRIPTION : Bulk insert data into the database using SqlBulkCopy
        // PARAMETERS  : List<YoYoModel> messages
        // RETURNS     : bool
        private static async Task<bool> BulkStoreToDatabase(List<YoYoModel> messages)
        {
            try
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    // open a database connection asynchronously
                    await connection.OpenAsync();

                    using (var bulkCopy = new SqlBulkCopy(connection))
                    {
                        // match the table to the database table
                        bulkCopy.DestinationTableName = "YoYoProduction";

                        // map the data to the database columns
                        bulkCopy.ColumnMappings.Add("WorkArea", "WorkArea");
                        bulkCopy.ColumnMappings.Add("SerialNumber", "SerialNumber");
                        bulkCopy.ColumnMappings.Add("LineNumber", "LineNumber");
                        bulkCopy.ColumnMappings.Add("State", "State");
                        bulkCopy.ColumnMappings.Add("Reason", "Reason");
                        bulkCopy.ColumnMappings.Add("Timestamp", "Timestamp");
                        bulkCopy.ColumnMappings.Add("ProductID", "ProductID");

                        // convert the list to a data table for bulk inserting
                        DataTable dataTable = ConvertToDataTable(messages);

                        // bulk inser the data
                        await bulkCopy.WriteToServerAsync(dataTable);
                    }

                    Console.WriteLine($"Inserted {messages.Count} to the database");
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Database error: {ex.Message}");
                return false;
            }
        }

        // FUNCTION    : ConvertToDataTable
        // DESCRIPTION : Conver the message buffer list into a data table for bulk insertion
        // PARAMETERS  : List<YoYoModel> messages
        // RETURNS     : dt DataTable
        private static DataTable ConvertToDataTable(List<YoYoModel> messages)
        {
            DataTable dt = new DataTable();

            // add columns to datatable that matches the database schema
            dt.Columns.Add("WorkArea", typeof(string));
            dt.Columns.Add("SerialNumber", typeof(Guid));
            dt.Columns.Add("LineNumber", typeof(string));
            dt.Columns.Add("State", typeof(string));
            dt.Columns.Add("Reason", typeof(string));
            dt.Columns.Add("Timestamp", typeof(DateTime));
            dt.Columns.Add("ProductID", typeof(int));

            // store the data table with data
            foreach (var message in messages)
            {
                dt.Rows.Add(
                    message.WorkArea,
                    message.SerialNumber,
                    message.LineNumber,
                    message.State,
                    message.Reason,
                    message.TimeStamp,
                    message.ProductID
                    );
            }

            // return the data table with data
            return dt;
        }

        // FUNCTION    : GetMSMQPath
        // DESCRIPTION : Get the Microsoft Message Queue server path from the user
        // PARAMETERS  : Nothing
        // RETURNS     : Nothing
        private static string GetMSMQPath()
        {
            Console.WriteLine("Enter the Queue Server (blank for default): ");
            string txtQueueServer = Console.ReadLine();

            // check if the string is blank
            if (string.IsNullOrWhiteSpace(txtQueueServer))
            {
                // get the host machine path
                txtQueueServer = Environment.MachineName;
            }

            // format string for the MSMQ path
            string msmqPath = "Formatname:Direct=os:";
            string queueName = "\\private$\\yoyo";
            return msmqPath + txtQueueServer + queueName;
        }
    }
}
