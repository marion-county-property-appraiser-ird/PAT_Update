﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic;
using System.Data.SqlClient;

static class PAT_UpdateCSharp
{
    private static int rYear = 2022;
    private static string connStr = string.Format(@"Data Source=Merlin\MerlinSQL;Initial Catalog=CAMA{0};User ID=CamaUser;Password=mcpa27cama;", rYear);
    private static SqlConnection connMerlin = new SqlConnection(connStr);
    private static string connStr2 = "Data Source=ARCGIS2;Initial Catalog=mcpagis;User ID=sde;Password=life=6*9;";
    private static SqlConnection connMCPAGIS = new SqlConnection(connStr2);
    private static string connStrShared = string.Format(@"Data Source=Merlin\MerlinSQL;Initial Catalog=CAMA;User ID=CamaUser;Password=mcpa27cama;", rYear);
    private static SqlConnection connShared = new SqlConnection(connStrShared);

    public static void Main()
    {
        int x;

        string myCommand;
        myCommand = string.Format("select ParcelNumber, Name{0}.Primekey, Record, LastName, FirstName, MiddleName, Suffix, PrimaryName  from Name{0} Join MasterParcel{0} on Name{0}.PrimeKey = MasterParcel{0}.Primekey Where roll = 1 And status = 0 And specialuse = '' and confidential = ''", rYear);
        SqlDataAdapter myAdapter = new SqlDataAdapter(myCommand, connMerlin);
        DataSet names = new DataSet();
        myAdapter.Fill(names, "DATA");
        myAdapter.Dispose();

        myCommand = string.Format("select ParcelNumber, Month, Year, Price  from Sale Join CAMA{0}.dbo.MasterParcel{0} on Sale.PrimeKey = CAMA{0}.dbo.MasterParcel{0}.Primekey Where roll = 1 And status = 0 And specialuse = '' and confidential = '' and year > {0} - 4", rYear);
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
        myCommand = "delete CurrentName";
        SqlCommand com = new SqlCommand(myCommand, connMCPAGIS);
        connMCPAGIS.Open();
        com.ExecuteNonQuery();
        connMCPAGIS.Close();
        com.Dispose();

        for (x = 0; x <= names.Tables(0).Rows.Count - 1; x++)
        {
            string parcel = names.Tables(0).Rows(x).Item("ParcelNumber").ToString;
            int pkey = System.Convert.ToInt32(names.Tables(0).Rows(x).Item("PrimeKey"));
            int rec = System.Convert.ToInt32(names.Tables(0).Rows(x).Item("Record"));
            string lName = names.Tables(0).Rows(x).Item("LastName").ToString.Replace('\'', '`');
            string fName = names.Tables(0).Rows(x).Item("FirstName").ToString.Replace('\'', '`');
            string mName = names.Tables(0).Rows(x).Item("MiddleName").ToString.Replace('\'', '`');
            string suff = names.Tables(0).Rows(x).Item("Suffix").ToString.Replace('\'', '`');
            int pName = System.Convert.ToInt32(names.Tables(0).Rows(x).Item("PrimaryName"));

            myCommand = string.Format("insert into CurrentName (ParcelNumber, PrimeKey, Record, LastName, FirstName, MiddleName, Suffix, PrimaryName) VALUES ('{0}', {1}, {2}, '{3}', '{4}', '{5}', '{6}', {7})", parcel, pkey, rec, lName, fName, mName, suff, pName);
            com = new SqlCommand(myCommand, connMCPAGIS);
            connMCPAGIS.Open();
            com.ExecuteNonQuery();
            connMCPAGIS.Close();
            com.Dispose();
        }
        names.Dispose();

        // Process sales
        myCommand = "delete CurrentSale";
        com = new SqlCommand(myCommand, connMCPAGIS);
        connMCPAGIS.Open();
        com.ExecuteNonQuery();
        connMCPAGIS.Close();
        com.Dispose();

        for (x = 0; x <= sales.Tables(0).Rows.Count - 1; x++)
        {
            string parcel = sales.Tables(0).Rows(x).Item("ParcelNumber").ToString;
            int mnth = System.Convert.ToInt32(sales.Tables(0).Rows(x).Item("Month"));
            int yr = System.Convert.ToInt32(sales.Tables(0).Rows(x).Item("Year"));
            int price = System.Convert.ToInt32(sales.Tables(0).Rows(x).Item("Price"));
            string dte = mnth.ToString().PadLeft(2, '0') + "/1/" + yr.ToString();
            myCommand = string.Format("insert into CurrentSale (ParcelNumber, Month, Year, Price, SaleDate) VALUES ('{0}', {1}, {2}, {3}, '{4}')", parcel, mnth, yr, price, dte);
            com = new SqlCommand(myCommand, connMCPAGIS);
            connMCPAGIS.Open();
            com.ExecuteNonQuery();
            connMCPAGIS.Close();
            com.Dispose();
        }
        sales.Dispose();

        // Process situs
        myCommand = "delete CurrentSitus";
        com = new SqlCommand(myCommand, connMCPAGIS);
        connMCPAGIS.Open();
        com.ExecuteNonQuery();
        connMCPAGIS.Close();
        com.Dispose();

        for (x = 0; x <= situses.Tables(0).Rows.Count - 1; x++)
        {
            string parcel = situses.Tables(0).Rows(x).Item("ParcelNumber").ToString;
            int pkey = System.Convert.ToInt32(situses.Tables(0).Rows(x).Item("Primekey"));
            int rec = System.Convert.ToInt32(situses.Tables(0).Rows(x).Item("Record"));
            int bNbr = System.Convert.ToInt32(situses.Tables(0).Rows(x).Item("BldgNbr"));
            int hNbr = System.Convert.ToInt32(situses.Tables(0).Rows(x).Item("HouseNbr"));
            string quad = situses.Tables(0).Rows(x).Item("Quadrant").ToString;
            string street = situses.Tables(0).Rows(x).Item("Street").ToString.Replace('\'', '`');
            string city = situses.Tables(0).Rows(x).Item("City").ToString.Replace('\'', '`');
            string zip = situses.Tables(0).Rows(x).Item("Zip").ToString;
            string sType = situses.Tables(0).Rows(x).Item("StructureType").ToString.Replace('\'', '`');
            myCommand = string.Format("insert into CurrentSitus (ParcelNumber, PrimeKey, Record, BldgNbr, HouseNbr, Quadrant, Street, City, Zip, StructureType) VALUES ('{0}', {1}, {2}, {3}, {4}, '{5}', '{6}', '{7}', '{8}', '{9}')", parcel, pkey, rec, bNbr, hNbr, quad, street, city, zip, sType);
            com = new SqlCommand(myCommand, connMCPAGIS);
            connMCPAGIS.Open();
            com.ExecuteNonQuery();
            connMCPAGIS.Close();
            com.Dispose();
        }
        situses.Dispose();
    }
}
