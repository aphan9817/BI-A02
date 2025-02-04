/*
* FILE : program.cs
* PROJECT : SENG3120 - Assignment #2
* PROGRAMMER : Anthony Phan & Jake Warywoda
* FIRST VERSION : 2025-01-31
* DESCRIPTION :
* The functions in this file are used to …
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
        private static readonly string connectionString = "Server=184.146.173.161\\SQLExpress,1300;Database=YoYoDB;User Id=Anthony;Password=YoYo1300;Encrypt=True;TrustServerCertificate=True;";
        private static List<YoYoModel> messages = new List<YoYoModel>();
        private const int BatchSize = 5;

        static async Task Main(string[] args)
        {

            Console.WriteLine("Queue reader starting");
            string msmqPath = GetMSMQPath();

            try
            {

                using (var msmq = new MessageQueue(msmqPath))
                {
                    msmq.Formatter = new ActiveXMessageFormatter();
                    Console.WriteLine($"Connected to queue: {msmqPath}");

                    Console.WriteLine("press 's' to start, 'q' to quit");

                    while (true)
                    {
                        var input = Console.ReadKey(intercept: true).KeyChar;

                        if (input == 's')
                        {
                            if (!bRead)
                            {
                                Console.WriteLine("Reading messages..");
                                bRead = true;

                                await ReadMSMQ(msmq);
                            }
                            else
                            {
                                Console.WriteLine($"reading {input}");
                            }
                        }
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

        // FUNCTION    : myFirstFunction
        // DESCRIPTION :
        // PARAMETERS  :
        // RETURNS     :
        private static async Task ReadMSMQ(MessageQueue msmq)
        {
            while (bRead)
            {
                try
                {
                    var message = await Task.Factory.FromAsync(
                        msmq.BeginReceive(),
                        msmq.EndReceive);

                    string messageBody = message.Body.ToString();
                    Console.WriteLine($"Received: {messageBody}");

                    var yoyoModel = ParseMessage(messageBody);
                    messages.Add(yoyoModel);

                    if (messages.Count >= BatchSize)
                    {
                        bool success = await BulkStoreToDatabase(messages);

                        if (!success)
                        {
                            Console.WriteLine("failed to store to database");
                            bRead = false;
                        }
                        messages.Clear();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unhandled Exception: {ex.Message}");
                }

                await Task.Delay(1000);
            }
        }


        private static YoYoModel ParseMessage(string message)
        {
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

        private static async Task<bool> BulkStoreToDatabase(List<YoYoModel> messages)
        {
            try
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();

                    using (var bulkCopy = new SqlBulkCopy(connection))
                    {
                        bulkCopy.DestinationTableName = "YoYoProduction";

                        bulkCopy.ColumnMappings.Add("WorkArea", "WorkArea");
                        bulkCopy.ColumnMappings.Add("SerialNumber", "SerialNumber");
                        bulkCopy.ColumnMappings.Add("LineNumber", "LineNumber");
                        bulkCopy.ColumnMappings.Add("State", "State");
                        bulkCopy.ColumnMappings.Add("Reason", "Reason");
                        bulkCopy.ColumnMappings.Add("Timestamp", "Timestamp");
                        bulkCopy.ColumnMappings.Add("ProductID", "ProductID");

                        DataTable dataTable = ConvertToDataTable(messages);
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

        private static DataTable ConvertToDataTable(List<YoYoModel> messages)
        {
            DataTable dt = new DataTable();

            dt.Columns.Add("WorkArea", typeof(string));
            dt.Columns.Add("SerialNumber", typeof(Guid));
            dt.Columns.Add("LineNumber", typeof(string));
            dt.Columns.Add("State", typeof(string));
            dt.Columns.Add("Reason", typeof(string));
            dt.Columns.Add("Timestamp", typeof(DateTime));
            dt.Columns.Add("ProductID", typeof(int));

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

            return dt;
        }

        private static string GetMSMQPath()
        {
            Console.WriteLine("Enter the Queue Server (blank for default): ");
            string txtQueueServer = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(txtQueueServer))
            {
                txtQueueServer = Environment.MachineName;
            }

            string msmqPath = "Formatname:Direct=os:";
            string queueName = "\\private$\\yoyo";
            return msmqPath + txtQueueServer + queueName;
        }
    }
}
