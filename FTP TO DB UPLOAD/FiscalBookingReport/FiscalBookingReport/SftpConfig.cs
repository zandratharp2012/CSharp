// SftpConfig.cs
namespace FiscalBookingReport.Config
{

    public class SftpConfig
    {
        public string host { get; set;}
        public string username { get; set;}
        public string password { get; set;}
        public string remoteDirectory { get; set;}

        public static SftpConfig Load()
        {
            // You can load these from environment variables, appsettings.json, or here directly:
            return new SftpConfig
            {
                host = "host",
                username = "username",
                password = "password",
                remoteDirectory = "directory"
            };
        }
    }
}