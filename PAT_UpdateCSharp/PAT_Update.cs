using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace PAT_UpdateCSharp
{
    internal class PAT_Update
    {
        private static int rYear = 2025;
        private static string connStr = string.Format(@"Data Source=Merlin\MerlinSQL;Initial Catalog=CAMA{0};User ID=CamaUser;Password=mcpa27cama;", rYear);
        private static SqlConnection connMerlin = new SqlConnection(connStr);

        private static string connStr2 = "Data Source=ARCGIS2;Initial Catalog=mcpagis;User ID=sde;Password=life=6*9;";
        private static SqlConnection connMCPAGIS = new SqlConnection(connStr2);

        private static string connStrShared = string.Format(@"Data Source=Merlin\MerlinSQL;Initial Catalog=CAMA;User ID=CamaUser;Password=mcpa27cama;", rYear);
        private static SqlConnection connShared = new SqlConnection(connStrShared);

        //private static string connStrCT = string.Format(@"Data Source=Merlin\MerlinSQL;Initial Catalog=CAMA;User ID=CamaUser;Password=mcpa27cama;", rYear);
        //private static SqlConnection connCT = new SqlConnection(connStrCT);

        static void Main()
        {
            int x;
            System.IO.File.AppendAllText(String.Format(@"{0}\MerlinLogFiles\PAT_Updater.log", Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments)), $"PAT_Update started at {DateTime.Now} \r\n");

            ////set up to choose correct roll year instead of hard coding. 
            //string myCommand;
            //myCommand = string.Format("Select * from OfficeInfoCT where Year = {0}", rYear);

            //SqlDataAdapter myAdapter = new SqlDataAdapter(myCommand, connCT);
            //DataSet paInfo = new DataSet();
            //myAdapter.Fill(paInfo, "DATA");

            //rYear = System.Convert.ToInt32(paInfo.Tables[0].Rows[0]["TaxYear"]);
            //paInfo.Dispose();
            //myAdapter.Dispose();


            string myCommand;
            myCommand = string.Format("select ParcelNumber, Name{0}.Primekey, Record, LastName, FirstName, MiddleName, Suffix, PrimaryName  from Name{0} Join MasterParcel{0} on Name{0}.PrimeKey = MasterParcel{0}.Primekey Where roll = 1 And status = 0 And specialuse = '' and confidential = ''", rYear);
            SqlDataAdapter myAdapter = new SqlDataAdapter(myCommand, connMerlin);
            DataSet names = new DataSet();
            myAdapter.Fill(names, "DATA");
            myAdapter.Dispose();

            myCommand = string.Format("select ParcelNumber, Month, Day, Year, Price  from Sale Join CAMA{0}.dbo.MasterParcel{0} on Sale.PrimeKey = CAMA{0}.dbo.MasterParcel{0}.Primekey Where roll = 1 And status = 0 And specialuse = '' and confidential = '' and year > {0} - 4", rYear);
            myAdapter = new SqlDataAdapter(myCommand, connShared);
            DataSet sales = new DataSet();
            myAdapter.Fill(sales, "DATA");
            myAdapter.Dispose();

            myCommand = string.Format("select ParcelNumber, SitusAddress{0}.primekey, record, bldgnbr, housenbr, quadrant, street, city, zip, structuretype  from SitusAddress{0} Join MasterParcel{0} on SitusAddress{0}.PrimeKey = MasterParcel{0}.Primekey Where roll = 1 And status = 0 And specialuse = '' and confidential = ''", rYear);
            myAdapter = new SqlDataAdapter(myCommand, connMerlin);
            DataSet situses = new DataSet();
            myAdapter.Fill(situses, "DATA");
            myAdapter.Dispose();


            // 'Process names
            SqlCommand com = new SqlCommand("delete CurrentName", connMCPAGIS);
            connMCPAGIS.Open();
            com.ExecuteNonQuery();
            connMCPAGIS.Close();
            com.Dispose();

            for (x = 0; x < names.Tables[0].Rows.Count; x++)
            {
                string parcel = names.Tables[0].Rows[x]["ParcelNumber"].ToString();
                int pkey = System.Convert.ToInt32(names.Tables[0].Rows[x]["PrimeKey"]);
                int rec = System.Convert.ToInt32(names.Tables[0].Rows[x]["Record"]);
                string lName = names.Tables[0].Rows[x]["LastName"].ToString().Replace('\'', '`');
                string fName = names.Tables[0].Rows[x]["FirstName"].ToString().Replace('\'', '`');
                string mName = names.Tables[0].Rows[x]["MiddleName"].ToString().Replace('\'', '`');
                string suff = names.Tables[0].Rows[x]["Suffix"].ToString().Replace('\'', '`');
                int pName = System.Convert.ToInt32(names.Tables[0].Rows[x]["PrimaryName"]);

                myCommand = string.Format("insert into CurrentName (ParcelNumber, PrimeKey, Record, LastName, FirstName, MiddleName, Suffix, PrimaryName) VALUES ('{0}', {1}, {2}, '{3}', '{4}', '{5}', '{6}', {7})", parcel, pkey, rec, lName, fName, mName, suff, pName);
                com = new SqlCommand(myCommand, connMCPAGIS);
                connMCPAGIS.Open();
                com.ExecuteNonQuery();
                connMCPAGIS.Close();
                com.Dispose();
            }
            names.Dispose();
            System.IO.File.AppendAllText(String.Format(@"{0}\MerlinLogFiles\PAT_Updater.log", Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments)), $"   Names have been updated. {DateTime.Now} \r\n");
            Console.WriteLine("Processed Names");


            // Process sales
            com = new SqlCommand("delete CurrentSale", connMCPAGIS);
            connMCPAGIS.Open();
            com.ExecuteNonQuery();
            connMCPAGIS.Close();
            com.Dispose();

            for (x = 0; x < sales.Tables[0].Rows.Count; x++)
            {
                string parcel = sales.Tables[0].Rows[x]["ParcelNumber"].ToString();
                int mnth = System.Convert.ToInt32(sales.Tables[0].Rows[x]["Month"]);
                int day = System.Convert.ToInt32(sales.Tables[0].Rows[x]["Day"]);
                int yr = System.Convert.ToInt32(sales.Tables[0].Rows[x]["Year"]);
                int price = System.Convert.ToInt32(sales.Tables[0].Rows[x]["Price"]);

                if (day == 0) { day = 1; }

                string dte = mnth.ToString().PadLeft(2, '0') + "/" + day + "/" + yr.ToString();

                myCommand = string.Format("insert into CurrentSale (ParcelNumber, Month, Year, Price, SaleDate) VALUES ('{0}', {1}, {2}, {3}, '{4}')", parcel, mnth, yr, price, dte);
                com = new SqlCommand(myCommand, connMCPAGIS);
                connMCPAGIS.Open();
                com.ExecuteNonQuery();
                connMCPAGIS.Close();
                com.Dispose();
            }
            sales.Dispose();
            System.IO.File.AppendAllText(String.Format(@"{0}\MerlinLogFiles\PAT_Updater.log", Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments)), $"   Sales have been updated. {DateTime.Now} \r\n");
            Console.WriteLine("Processed Sales");

            // Process situs
            com = new SqlCommand("delete CurrentSitus", connMCPAGIS);
            connMCPAGIS.Open();
            com.ExecuteNonQuery();
            connMCPAGIS.Close();
            com.Dispose();

            for (x = 0; x < situses.Tables[0].Rows.Count; x++)
            {
                string parcel = situses.Tables[0].Rows[x]["ParcelNumber"].ToString();
                int pkey = System.Convert.ToInt32(situses.Tables[0].Rows[x]["Primekey"]);
                int rec = System.Convert.ToInt32(situses.Tables[0].Rows[x]["Record"]);
                int bNbr = System.Convert.ToInt32(situses.Tables[0].Rows[x]["BldgNbr"]);
                int hNbr = System.Convert.ToInt32(situses.Tables[0].Rows[x]["HouseNbr"]);
                string quad = situses.Tables[0].Rows[x]["Quadrant"].ToString();
                string street = situses.Tables[0].Rows[x]["Street"].ToString().Replace('\'', '`');
                string city = situses.Tables[0].Rows[x]["City"].ToString().Replace('\'', '`');
                string zip = situses.Tables[0].Rows[x]["Zip"].ToString();
                string sType = situses.Tables[0].Rows[x]["StructureType"].ToString().Replace('\'', '`');
                myCommand = string.Format("insert into CurrentSitus (ParcelNumber, PrimeKey, Record, BldgNbr, HouseNbr, Quadrant, Street, City, Zip, StructureType) VALUES ('{0}', {1}, {2}, {3}, {4}, '{5}', '{6}', '{7}', '{8}', '{9}')", parcel, pkey, rec, bNbr, hNbr, quad, street, city, zip, sType);
                com = new SqlCommand(myCommand, connMCPAGIS);
                connMCPAGIS.Open();
                com.ExecuteNonQuery();
                connMCPAGIS.Close();
                com.Dispose();
            }
            situses.Dispose();
            System.IO.File.AppendAllText(String.Format(@"{0}\MerlinLogFiles\PAT_Updater.log", Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments)), $"   Situs' have been updated. {DateTime.Now} \r\n");
            Console.WriteLine("Processed Situs");

            Environment.Exit(0);
        }
    }
}
