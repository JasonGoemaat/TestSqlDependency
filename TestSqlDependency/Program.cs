using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestSqlDependency
{
    class Program
    {
        static string QUERY = "SELECT ID, Title, Message FROM dbo.Messages ORDER BY ID DESC";

        static void Main(string[] args)
        {
            int count = 0;

            //// this really only needs to be executed once in the database
            using (SqlConnection cnn = new SqlConnection("Data Source=localhost;Integrated Security=SSPI")) 
            {
                cnn.Open();

                // create database if it doesn't exist
                using (SqlCommand cmd = new SqlCommand("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'TEST') CREATE DATABASE TEST", cnn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (SqlCommand cmd = new SqlCommand("DECLARE @enabled int SELECT @enabled = is_broker_enabled FROM sys.databases WHERE name = 'TEST' IF @enabled = 0 ALTER DATABASE TEST SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE", cnn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (SqlCommand cmd = new SqlCommand("USE TEST; IF OBJECT_ID('Messages') IS NULL CREATE TABLE Messages (ID int IDENTITY(1, 1) PRIMARY KEY,Title varchar(255),Message varchar(255))", cnn))
                {
                    cmd.ExecuteNonQuery();
                }
            }

            SqlDependency.Start(ConfigurationManager.ConnectionStrings["Test"].ConnectionString);


            Console.WriteLine("Initial results:");
            ShowMessages(); // will call event if values change

            Console.WriteLine("Listening for changes to Messages, ESC to exit, SPACE to create a new message");
            ConsoleKeyInfo keyInfo = Console.ReadKey();
            while (keyInfo.Key != ConsoleKey.Escape)
            {
                if (keyInfo.Key == ConsoleKey.Spacebar)
                {
                    Console.WriteLine("Inserting...");
                    using (SqlConnection cnn = GetConnection())
                    {
                        using (SqlCommand cmd = new SqlCommand("INSERT INTO Messages(Title, Message) VALUES (@Title, @Message)", cnn))
                        {
                            cmd.Parameters.AddWithValue("@Title", string.Format("Message {0:X4}", count++));
                            cmd.Parameters.AddWithValue("@Message", string.Format("Added at {0:yyyy-MM-dd HH:mm:ss}", DateTime.Now));
                            cmd.ExecuteNonQuery();
                        }
                    }

                } else
                {
                    Console.WriteLine("Unknown keypress: {0}, press ESC to exit or SPACE to insert message", keyInfo);
                }
                keyInfo = Console.ReadKey();
            }
            Console.WriteLine("You pressed ESC, exiting...");
        }

        // Handler method
        static void OnDependencyChange(object sender, SqlNotificationEventArgs e)
        {
            Console.WriteLine("Dependency change!");
            ShowMessages();
        }

        static SqlConnection GetConnection()
        {
            SqlConnection cnn = new SqlConnection(ConfigurationManager.ConnectionStrings["Test"].ConnectionString);
            cnn.Open();
            return cnn;
        }
        static void ShowMessages()
        {
            using (SqlConnection cnn = GetConnection())
            {
                using (SqlCommand command = new SqlCommand(QUERY, cnn))
                {
                    SqlDependency dependency = new SqlDependency(command);
                    dependency.OnChange += new OnChangeEventHandler(OnDependencyChange);
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        int count = 0;
                        while (reader.Read() && count++ < 5)
                        {
                            Console.WriteLine("Message {0} \"{1}\": {2}", reader.GetInt32(0), reader.GetString(1), reader.GetString(2));
                        }
                    }
                }
            }
        }
    }
}
