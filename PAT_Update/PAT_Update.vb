Imports System.Data.SqlClient
Imports System.Runtime.InteropServices

Module PAT_Update
    Private rYear As Integer = 2022
    Private connStr As String = String.Format("Data Source=Merlin\MerlinSQL;Initial Catalog=CAMA{0};User ID=CamaUser;Password=mcpa27cama;", rYear)
    Private connMerlin As New SqlConnection(connStr)
    Private connStr2 As String = "Data Source=ARCGIS2;Initial Catalog=mcpagis;User ID=sde;Password=life=6*9;"
    Private connMCPAGIS As New SqlConnection(connStr2)
    Private connStrShared As String = String.Format("Data Source=Merlin\MerlinSQL;Initial Catalog=CAMA;User ID=CamaUser;Password=mcpa27cama;", rYear)
    Private connShared As New SqlConnection(connStrShared)

    Sub Main()
        Dim x As Integer

        Dim myCommand As String
        myCommand = String.Format("select ParcelNumber, Name{0}.Primekey, Record, LastName, FirstName, MiddleName, Suffix, PrimaryName  from Name{0} Join MasterParcel{0} on Name{0}.PrimeKey = MasterParcel{0}.Primekey Where roll = 1 And status = 0 And specialuse = '' and confidential = ''", rYear)
        Dim myAdapter As SqlDataAdapter = New SqlDataAdapter(myCommand, connMerlin)
        Dim names As New DataSet
        myAdapter.Fill(names, "DATA")
        myAdapter.Dispose()

        myCommand = String.Format("select ParcelNumber, Month, Year, Price  from Sale Join CAMA{0}.dbo.MasterParcel{0} on Sale.PrimeKey = CAMA{0}.dbo.MasterParcel{0}.Primekey Where roll = 1 And status = 0 And specialuse = '' and confidential = '' and year > {0} - 4", rYear)
        myAdapter = New SqlDataAdapter(myCommand, connShared)
        Dim sales As New DataSet
        myAdapter.Fill(sales, "DATA")
        myAdapter.Dispose()

        myCommand = String.Format("select ParcelNumber, SitusAddress{0}.primekey, record, bldgnbr, housenbr, quadrant, street, city, zip, structuretype  from SitusAddress{0} Join MasterParcel{0} on SitusAddress{0}.PrimeKey = MasterParcel{0}.Primekey Where roll = 1 And status = 0 And specialuse = '' and confidential = ''", rYear)
        myAdapter = New SqlDataAdapter(myCommand, connMerlin)
        Dim situses As New DataSet
        myAdapter.Fill(situses, "DATA")
        myAdapter.Dispose()

        ''Process names
        myCommand = "delete CurrentName"
        Dim com As New SqlCommand(myCommand, connMCPAGIS)
        connMCPAGIS.Open()
        com.ExecuteNonQuery()
        connMCPAGIS.Close()
        com.Dispose()

        For x = 0 To names.Tables(0).Rows.Count - 1
            Dim parcel As String = names.Tables(0).Rows(x).Item("ParcelNumber").ToString
            Dim pkey As Integer = CInt(names.Tables(0).Rows(x).Item("PrimeKey"))
            Dim rec As Integer = CInt(names.Tables(0).Rows(x).Item("Record"))
            Dim lName As String = names.Tables(0).Rows(x).Item("LastName").ToString.Replace("'"c, "`"c)
            Dim fName As String = names.Tables(0).Rows(x).Item("FirstName").ToString.Replace("'"c, "`"c)
            Dim mName As String = names.Tables(0).Rows(x).Item("MiddleName").ToString.Replace("'"c, "`"c)
            Dim suff As String = names.Tables(0).Rows(x).Item("Suffix").ToString.Replace("'"c, "`"c)
            Dim pName As Integer = CInt(names.Tables(0).Rows(x).Item("PrimaryName"))

            myCommand = String.Format("insert into CurrentName (ParcelNumber, PrimeKey, Record, LastName, FirstName, MiddleName, Suffix, PrimaryName) VALUES ('{0}', {1}, {2}, '{3}', '{4}', '{5}', '{6}', {7})", parcel, pkey, rec, lName, fName, mName, suff, pName)
            com = New SqlCommand(myCommand, connMCPAGIS)
            connMCPAGIS.Open()
            com.ExecuteNonQuery()
            connMCPAGIS.Close()
            com.Dispose()
        Next
        names.Dispose()
        My.Computer.FileSystem.WriteAllText(String.Format("{0}\MerlinLogFiles\PAT_Updater.log", Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments)), "Names have been updated." & "  " & DateTime.Now.ToShortDateString & vbCrLf, True)


        'Process sales
        myCommand = "delete CurrentSale"
        com = New SqlCommand(myCommand, connMCPAGIS)
        connMCPAGIS.Open()
        com.ExecuteNonQuery()
        connMCPAGIS.Close()
        com.Dispose()

        For x = 0 To sales.Tables(0).Rows.Count - 1
            Dim parcel As String = sales.Tables(0).Rows(x).Item("ParcelNumber").ToString
            Dim mnth As Integer = CInt(sales.Tables(0).Rows(x).Item("Month"))
            Dim yr As Integer = CInt(sales.Tables(0).Rows(x).Item("Year"))
            Dim price As Integer = CInt(sales.Tables(0).Rows(x).Item("Price"))
            Dim dte As String = mnth.ToString.PadLeft(2, "0"c) + "/1/" + yr.ToString
            myCommand = String.Format("insert into CurrentSale (ParcelNumber, Month, Year, Price, SaleDate) VALUES ('{0}', {1}, {2}, {3}, '{4}')", parcel, mnth, yr, price, dte)
            com = New SqlCommand(myCommand, connMCPAGIS)
            connMCPAGIS.Open()
            com.ExecuteNonQuery()
            connMCPAGIS.Close()
            com.Dispose()
        Next
        sales.Dispose()
        My.Computer.FileSystem.WriteAllText(String.Format("{0}\MerlinLogFiles\PAT_Updater.log", Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments)), "Sales have been updated." & "  " & DateTime.Now.ToShortDateString & vbCrLf, True)


        'Process situs
        myCommand = "delete CurrentSitus"
        com = New SqlCommand(myCommand, connMCPAGIS)
        connMCPAGIS.Open()
        com.ExecuteNonQuery()
        connMCPAGIS.Close()
        com.Dispose()

        For x = 0 To situses.Tables(0).Rows.Count - 1
            Dim parcel As String = situses.Tables(0).Rows(x).Item("ParcelNumber").ToString
            Dim pkey As Integer = CInt(situses.Tables(0).Rows(x).Item("Primekey"))
            Dim rec As Integer = CInt(situses.Tables(0).Rows(x).Item("Record"))
            Dim bNbr As Integer = CInt(situses.Tables(0).Rows(x).Item("BldgNbr"))
            Dim hNbr As Integer = CInt(situses.Tables(0).Rows(x).Item("HouseNbr"))
            Dim quad As String = situses.Tables(0).Rows(x).Item("Quadrant").ToString
            Dim street As String = situses.Tables(0).Rows(x).Item("Street").ToString.Replace("'"c, "`"c)
            Dim city As String = situses.Tables(0).Rows(x).Item("City").ToString.Replace("'"c, "`"c)
            Dim zip As String = situses.Tables(0).Rows(x).Item("Zip").ToString
            Dim sType As String = situses.Tables(0).Rows(x).Item("StructureType").ToString.Replace("'"c, "`"c)
            myCommand = String.Format("insert into CurrentSitus (ParcelNumber, PrimeKey, Record, BldgNbr, HouseNbr, Quadrant, Street, City, Zip, StructureType) VALUES ('{0}', {1}, {2}, {3}, {4}, '{5}', '{6}', '{7}', '{8}', '{9}')", parcel, pkey, rec, bNbr, hNbr, quad, street, city, zip, sType)
            com = New SqlCommand(myCommand, connMCPAGIS)
            connMCPAGIS.Open()
            com.ExecuteNonQuery()
            connMCPAGIS.Close()
            com.Dispose()
        Next
        situses.Dispose()
        My.Computer.FileSystem.WriteAllText(String.Format("{0}\MerlinLogFiles\PAT_Updater.log", Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments)), "Situs' have been updated." & "  " & DateTime.Now.ToShortDateString & vbCrLf, True)

    End Sub
End Module
