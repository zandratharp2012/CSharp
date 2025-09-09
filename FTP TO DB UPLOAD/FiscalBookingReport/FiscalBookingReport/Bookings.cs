using System;
using System.Data;
//using System.Globalization;
//using System.IO;
//using System.Linq;
//using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using Dapper;
using ClosedXML.Excel;
using System.Net;
using Renci.SshNet;
using FiscalBookingReport.Config;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Graph;
using Microsoft.Identity.Client;
using Azure.Identity;
class Program
{
    static void Main(string[] args)
    {
        // Start stopwatch to time the process
        var stopwatch = Stopwatch.StartNew();
        string statusMessage = "Unknown status";
        string tableName = "FISCAL_BOOKING_REPORT_STAGING";
        string tableName2 = "FISCAL_BOOKING_REPORT_STAGING_4RF";
        // Load SFTP configuration from external config
        var config = SftpConfig.Load();  
        string connectionString = "Server=[Server Name Goes Here];Database=[DB Name Goes Here];Trusted_Connection=True;TrustServerCertificate=True;";

        try
        {   
            DataTable dataTableSFTP = DownloadFileFromSftp();
            DataTable dataTableSharepoint = DownloadFileFromSharePoint().GetAwaiter().GetResult();
  
            if (dataTableSFTP != null && dataTableSFTP.Rows.Count > 0)
            {
                using (var conn = new SqlConnection(connectionString))
                {
                    conn.Open();
                    var dbColumns = GetTableColumns(conn, tableName2);
                    var matchingColumns = dataTableSFTP.Columns.Cast<DataColumn>()
                        .Select(col => col.ColumnName)
                        .Where(colName => dbColumns.Contains(colName, StringComparer.OrdinalIgnoreCase))
                        .ToList();
                    InsertDataIntoDatabase(conn, tableName, dataTableSFTP, matchingColumns);
                }
            }
            else
            {
                string SFTPMessage = $"ERROR: {tableName} datatable is null or contains 0 rows.";
                using (var conn = new SqlConnection(connectionString))
                {
                    conn.Open();
                    var logParamsSFTPError = new
                    {
                        LogDate = DateTime.Now,
                        Description = "Error",
                        Source = "FISCAL BOOKING REPORT STAGING",
                        TableName = tableName,
                        Message = SFTPMessage
                    };

                    string logInsertQuerySFTPError = $"INSERT INTO DWLog (actionTime, Description, Source,TableName, Message) " +
                        $"VALUES (@LogDate, @Description, @Source, @TableName,@Message)";
                    conn.Execute(logInsertQuerySFTPError, logParamsSFTPError);

                }
            }

            
            if (dataTableSharepoint != null && dataTableSharepoint.Rows.Count > 0)
            {
                using (var conn = new SqlConnection(connectionString))
                {
                    conn.Open();
                    var dbColumns = GetTableColumns(conn, tableName);
                    var matchingColumns = dataTableSharepoint.Columns.Cast<DataColumn>()
                        .Select(col => col.ColumnName)
                        .Where(colName => dbColumns.Contains(colName, StringComparer.OrdinalIgnoreCase))
                        .ToList();
                    InsertDataIntoDatabase(conn, tableName2, dataTableSharepoint, matchingColumns);
                }
            }
            else
            {
                string SPMessage = $"ERROR: {tableName2} datatable is null or contains 0 rows.";
                using (var conn = new SqlConnection(connectionString))
                {
                    conn.Open();
                    var logParamsSPError = new
                    {
                        LogDate = DateTime.Now,
                        Description = "Error",
                        Source = "FISCAL BOOKING REPORT STAGING",
                        TableName = tableName2,
                        Message = SPMessage
                    };

                    string logInsertQuerySPError = $"INSERT INTO DWLog (actionTime, Description, Source,TableName, Message) " +
                        $"VALUES (@LogDate, @Description, @Source, @TableName,@Message)";
                    conn.Execute(logInsertQuerySPError, logParamsSPError);

                }

            }
        }
        catch (Exception ex)
        {
            // Catch any top-level errors during processing
            string errorMessage = $"ERROR: {ex.Message}";
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var logParamsError = new
                {
                    LogDate = DateTime.Now,
                    Description = "Error",
                    Source = "FISCAL BOOKING REPORT STAGING",
                    Message = errorMessage
                };

                string logInsertQueryError = $"INSERT INTO DWLog (actionTime, Description, Source, Message) " +
                    $"VALUES (@LogDate, @Description, @Source, @Message)";
                conn.Execute(logInsertQueryError, logParamsError);

            }
        }
    
    }

 
    // Helper to retrieve column names of a SQL Server table
    static HashSet<string> GetTableColumns(SqlConnection conn, string tableName)
    {
        string query = @"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TableName";
        return new HashSet<string>(conn.Query<string>(query, new { TableName = tableName }));
    }

    // Inserts Excel data into the SQL table inside a transaction
    static void InsertDataIntoDatabase(SqlConnection conn, string tableName, DataTable dataTable, List<string> matchingColumns)
    {
        var stopwatch = Stopwatch.StartNew();
        using (var transaction = conn.BeginTransaction())
        {
            try
            {
                // Delete all existing data in the target table (truncate logic)
                string deleteQuery = $"DELETE FROM {tableName}";
                conn.Execute(deleteQuery, transaction: transaction);
                Console.WriteLine("Existing data deleted from the table.");

                // Build dynamic insert query using only matched columns
                string columnNames = string.Join(", ", matchingColumns.Select(c => $"[{c}]"));
                string paramNames = string.Join(", ", matchingColumns.Select(c => $"@{c}"));

                string insertQuery = $"INSERT INTO {tableName} ({columnNames}) VALUES ({paramNames})";

                // Insert rows one by one
                foreach (DataRow row in dataTable.Rows)
                {
                    var parameters = matchingColumns.ToDictionary(col => col, col => ConvertValue(row[col]));
                    conn.Execute(insertQuery, parameters, transaction);
                }

                transaction.Commit();    
                string statusMessage = $"Data inserted successfully to {tableName}";
                Console.WriteLine(statusMessage);

                stopwatch.Stop();
                double durationMinutes = stopwatch.Elapsed.TotalMinutes;

                var logParams = new
                {
                    LogDate = DateTime.Now,
                    Description = "Load Complete",
                    Source = "FISCAL BOOKING REPORT STAGING",
                    TableName = tableName,
                    DurationMin = durationMinutes,
                    Message = statusMessage
                };

                string logInsertQuery = $"INSERT INTO DWLog (actionTime, Description, Source, TableName, Duration_Min, Message) " +
                    $"VALUES (@LogDate, @Description, @Source, @TableName, @DurationMin, @Message)";
                conn.Execute(logInsertQuery, logParams);

               if (tableName == "FISCAL_BOOKING_REPORT_STAGING")
                {
                    string UpdateFiscalBookingPROD = @"INSERT INTO dw.dbo.FISCAL_BOOKING_REPORT_PROD SELECT* FROM dw.dbo.v_FISCAL_BOOKING_REPORT_STAGING;";
                    conn.Execute(UpdateFiscalBookingPROD);
                }
               

            }
            catch (Exception ex)
            {
                transaction.Rollback();
                string errorMessage = ex.Message;   
                Console.WriteLine("Error inserting data: " + errorMessage);

                var logParamsError = new
                {
                    LogDate = DateTime.Now,
                    Description = "Error",
                    Source = "FISCAL BOOKING REPORT STAGING",
                    TableName = tableName,   
                    StatusMessage = errorMessage
                };

                string logInsertQueryError = $"INSERT INTO DWLog (actionTime, Description, Source, TableName, Message) " +
                    $"VALUES (@LogDate, @Description, @Source, @TableName, @StatusMessage)";
                conn.Execute(logInsertQueryError, logParamsError);

                
            }
        }
    }

    // Helper to convert value safely (e.g., trim strings, handle nulls)
    static object ConvertValue(object value)
    {
        if (value == DBNull.Value) return null;

        if (value is string strValue)
        {
            if (double.TryParse(strValue, out _))
                return strValue.Trim(); // Keep numeric strings intact
            return strValue.Trim();
        }

        return value;
    }

 

    public static async Task<DataTable> DownloadFileFromSharePoint()
    {

        // Azure AD application credentials (register your app in Azure portal)
        string clientId = "CLIENT ID";
        string tenantId = "TENANT ID";
        string clientSecret = "CLIENT SECRET";

        // Your SharePoint domain, usually yourtenant.sharepoint.com
        var sharepointDomain = "tenant.sharepoint.com";

        // The name of the site in the SharePoint URL
        var siteName = "FiscalBookingReport";

        // Path to the file inside the SharePoint document library
        // Format: "Shared Documents/<folder>/<file name>"
        var filePathInSharePoint = "Shared Documents/FiscalBookingReport/FiscalBookingTemplate29APR25.xlsx";

        // Where to save the downloaded file locally
        var localDownloadPath = "DownloadedFile.xlsx";

        // Create a credential object using client ID and secret
        var credential = new ClientSecretCredential(tenantId, clientId, clientSecret);

        // Create a GraphServiceClient to call Microsoft Graph API
        var graphClient = new GraphServiceClient(credential);

        try
        {
            Console.WriteLine("Getting SharePoint site...");

            var site = await graphClient.Sites[$"{sharepointDomain}:/sites/{siteName}"].GetAsync();
            //Console.WriteLine($"Site retrieved. Site ID: {site.Id}");

            // Step 2: Get the default document library (Drive) for the site
            var drive = await graphClient
                .Sites[site.Id]
                .Drive
                .GetAsync();

            //Console.WriteLine($"Drive retrieved. Drive ID: {drive.Id}");

            // Get the folder
            var folder = await graphClient.Drives[drive.Id]
                .Root
                .ItemWithPath("Fiscal Booking Report")
                .GetAsync();

            //Console.WriteLine($"Folder: {folder.Name}");

            // Get the files inside the folder
            var children = await graphClient.Drives[drive.Id]
                .Items[folder.Id]
                .Children
                .GetAsync();

            // Find the most recent file where the name contains "FiscalBookingReport4RF"
            var latestFile = children.Value
                .Where(i => i.File != null && i.Name.Contains("FiscalBookingReport4RF", StringComparison.OrdinalIgnoreCase))
                .OrderByDescending(i => i.CreatedDateTime)
                .FirstOrDefault();


            // Check if file is within 7 days
            if (latestFile == null || !latestFile.CreatedDateTime.HasValue ||
                (DateTime.Now - latestFile.CreatedDateTime.Value.DateTime).TotalDays > 7)
            {
                Console.WriteLine("No recent files found in the folder (within 7 days).");
                return null;
            }
            // Step 4: Download file content into memory
            using var fileStream = await graphClient.Drives[drive.Id]
                .Items[latestFile.Id]
                .Content
                .GetAsync();

            using var memoryStream = new MemoryStream();
            await fileStream.CopyToAsync(memoryStream);
            memoryStream.Position = 0;

            // Step 5: Read Excel into DataTable using ClosedXML
            using var workbook = new XLWorkbook(memoryStream);
            var worksheet = workbook.Worksheet(1); // First worksheet
            var dataTable = new DataTable();

            // Create DataTable columns based on header row
            foreach (var cell in worksheet.FirstRowUsed().CellsUsed())
            {
                dataTable.Columns.Add(cell.Value.ToString().Trim());
            }

            int colCount = dataTable.Columns.Count;

            // Add rows from Excel
            foreach (var row in worksheet.RowsUsed().Skip(1)) // skip header
            {
                var newRow = dataTable.NewRow();
                for (int i = 0; i < colCount; i++)
                {
                    newRow[i] = row.Cell(i + 1).Value;
                }
                dataTable.Rows.Add(newRow);
            }

            // Add UploadDate column
            dataTable.Columns.Add("UploadDate", typeof(DateTime));
            foreach (DataRow row in dataTable.Rows)
            {
                row["UploadDate"] = DateTime.Now;
            }

            Console.WriteLine($"DataTable created with {dataTable.Rows.Count} rows.");
            if (dataTable.Rows.Count > 0)
            {

                return dataTable;
            }
            else
            {
                Console.WriteLine("DataTable is empty — no rows to display.");
            }
        }

        catch (Microsoft.Graph.Models.ODataErrors.ODataError odataError)
        {
            Console.WriteLine($"OData Error: {odataError.Error?.Code} - {odataError.Error?.Message}");
        }
        catch (HttpRequestException httpEx)
        {
            Console.WriteLine($"HTTP Request Error: {httpEx.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"General Error: {ex.Message}");
            if (ex.InnerException != null)
            {
                Console.WriteLine($"Inner Exception: {ex.InnerException.Message}");
            }
        }
        return null;
    }
    // SFTP download and Excel-to-DataTable logic
static DataTable DownloadFileFromSftp()
{
    var config = SftpConfig.Load();
    string tableName = "FISCAL_BOOKING_REPORT_STAGING_4RF";
    string connectionString = "Server=[Server Name Goes Here];Database=[DB Name Goes Here];Trusted_Connection=True;TrustServerCertificate=True;";

    using (var sftp = new SftpClient(config.host, config.username, config.password))
    {
        sftp.Connect();
        var latestFile = sftp.ListDirectory(config.remoteDirectory)
            .Where(f => !f.IsDirectory && f.Name.StartsWith("FiscalWeekBookingVariance") && f.Name.EndsWith(".xlsx"))
            .OrderByDescending(f => f.LastWriteTime)
            .FirstOrDefault();

        if (latestFile == null || (DateTime.Now - latestFile.LastWriteTime).TotalDays > 7)
        {
                sftp.Disconnect();
                return null;
        }

        using (var memoryStream = new MemoryStream())
        {
            sftp.DownloadFile(latestFile.FullName, memoryStream);
            memoryStream.Position = 0;

            using (var workbook = new XLWorkbook(memoryStream))
            {
                var worksheet = workbook.Worksheet(1);
                DataTable dataTable = new DataTable();

                foreach (var cell in worksheet.FirstRowUsed().CellsUsed())
                {
                    dataTable.Columns.Add(cell.Value.ToString().Trim());
                }

                int colCount = dataTable.Columns.Count;

                foreach (var row in worksheet.RowsUsed().Skip(1))
                {
                    var newRow = dataTable.NewRow();
                    for (int i = 0; i < colCount; i++)
                    {
                        newRow[i] = row.Cell(i + 1).Value;
                    }
                    dataTable.Rows.Add(newRow);
                }

                dataTable.Columns.Add("UploadDate", typeof(DateTime));
                foreach (DataRow row in dataTable.Rows)
                {
                    row["UploadDate"] = DateTime.Now;
                }

                sftp.Disconnect();
                return dataTable;
            }
        }
    }
}

}


