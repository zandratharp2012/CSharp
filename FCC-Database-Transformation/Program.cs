using System;
using System.Data.SqlClient;
using System.IO;
using System.Net;
using System.Windows.Forms;
using System.IO.Compression;


namespace FCC_Database
{
    class FCC_Program
    {
        private static void Get_FTPData()
        {
            //string message = "We are at point A";
            //MessageBox.Show(message);

            //Fetch zip file from FCC database using FTP
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create("ftp://wirelessftp.fcc.gov/pub/uls/complete/l_micro.zip") as FtpWebRequest;

            request.Method = WebRequestMethods.Ftp.DownloadFile;
            //request.UsePassive = true;
            //request.UseBinary = true;

            //Check for response 
            FtpWebResponse response = (FtpWebResponse)request.GetResponse();

            Stream responseStream = response.GetResponseStream();
            StreamReader reader = new StreamReader(responseStream);

            using(FileStream writer = new FileStream(@"C:\FCC\l_micro.zip", FileMode.Create))
{

                long length = response.ContentLength;
                int bufferSize = 2048;
                int readCount;
                byte[] buffer = new byte[2048];

                readCount = responseStream.Read(buffer, 0, bufferSize);
                while (readCount > 0)
                {
                    writer.Write(buffer, 0, readCount);
                    readCount = responseStream.Read(buffer, 0, bufferSize);
                }
            }

            reader.Close();
            response.Close();

          //  ZipFile.CreateFromDirectory(startPath, zipPath);
            ZipFile.ExtractToDirectory(@"C:\FCC\l_micro.zip", @"C:\FCC\Extract");
        }

        private static SqlConnection  DW_connect()
        {
            MessageBox.Show(message4);
            SqlConnection myconn = new SqlConnection("Server=.\\[ServerNameGoesHere];Database=[YourDatabaseGoesHere];Integrated");
            myconn.Open();
            MessageBox.Show("Connection Open...");
            return myconn;
    
        }

        private static void Update_records()
        {
            string path = "";       //Path for zip files

            //Convert Files from .DAT to .Txt 

            string extract_Path = "";   //Path unzipped files (Change folder to Custom Apps desired folder)
          //  System.IO.Compression.ZipFile.ExtractToDirectory(path, extract_Path);

            //Convert Files from .DAT to .Txt 

            SqlConnection conn = DW_connect();//Sql Connection

            SqlCommand cmd = new SqlCommand(
               "Delete from PUBACC_AC;" +
               "Delete from PUBACC_ad;" +
               "Delete from PUBACC_am;" +
               "Delete from PUBACC_an;" +
               "Delete from PUBACC_as;" +
               "Delete from PUBACC_at;" +
               "Delete from PUBACC_bc;" +
               "Delete from PUBACC_bf;" +
               "Delete from PUBACC_bl;" +
               "Delete from PUBACC_bo;" +
               "Delete from PUBACC_cf;" +
               "Delete from PUBACC_cg;" +
               "Delete from PUBACC_co;" +
               "Delete from PUBACC_cp;" +
               "Delete from PUBACC_cs;" +
               "Delete from PUBACC_em;" +
               "Delete from PUBACC_en;" +
               "Delete from PUBACC_f2;" +
               "Delete from PUBACC_fa;" +
               "Delete from PUBACC_fc;" +
               "Delete from PUBACC_ff;" +
               "Delete from PUBACC_fr;" +
               "Delete from PUBACC_fs;" +
               "Delete from PUBACC_ft;" +
               "Delete from PUBACC_hd;" +
               "Delete from PUBACC_hs;" +
               "Delete from PUBACC_ia;" +
               "Delete from PUBACC_ir;" +
               "Delete from PUBACC_l2;" +
               "Delete from PUBACC_la;" +
               "Delete from PUBACC_lf;" +
               "Delete from PUBACC_lm;" +
               "Delete from PUBACC_lo;" +
               "Delete from PUBACC_mc;" +
               "Delete from PUBACC_mf;" +
               "Delete from PUBACC_mi;" +
               "Delete from PUBACC_mk;" +
               "Delete from PUBACC_mp;" +
               "Delete from PUBACC_mw;" +
               "Delete from PUBACC_op;" +
               "Delete from PUBACC_pa;" +
               "Delete from PUBACC_pc;" +
               "Delete from PUBACC_ra;" +
               "Delete from PUBACC_rc;" +
               "Delete from PUBACC_re;" +
               "Delete from PUBACC_rz;" +
               "Delete from PUBACC_sc;" +
               "Delete from PUBACC_se;" +
               "Delete from PUBACC_sf;" +
               "Delete from PUBACC_sg;" +
               "Delete from PUBACC_sh;" +
               "Delete from PUBACC_si;" +
               "Delete from PUBACC_sv;" +
               "Delete from PUBACC_ta;" +
               "Delete from PUBACC_tl;" +
               "Delete from PUBACC_ua;" +
               "Delete from PUBACC_vc;" 
               , conn);
               


            SqlDataReader reader = cmd.ExecuteReader();
            while (reader.Read())
            {
            Console.WriteLine("{0},{1},reader.GetInt32(0),reader.GetString(1)");
            }
            reader.Close();
            conn.Close();
            Console.ReadLine();



/*
 
            *******************************************************************
            Update MSO Records
            Upload records with MSOs to PCN database (UAT environment)
            SqlCommand cmd = new SqlCommand("SELECT * FROM PCN database where MSO is not blank);
            Select * from FCC databse where MSO is blank (this will be a new column in a created view)
            Take select columns from PCN lines such as frequency, coordinates, site name...etc...and create tuples         
            Take index of tuple and insert into respective command >>
            Example:
            insert into FCC_view (col1, . . . ,col8,col9,col10)
             select distinct col1, . . . ,col8, 'Step 3', col10
             from FCC_table
             where col1 LIKE '%{0}%'  
             AND col 2 = '{2}'
             AND col 3 ...etc...
            */
        }

        public static void Main(string[] args)
        {
            Get_FTPData();
            Update_records();
        }
    }

}


