// contains sample code from https://autofei.wordpress.com/2012/04/02/java-example-code-using-hbase-data-model-operations/

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.fs.Path;

public class hbase1
{
    private static Configuration conf = null;

    /**
     * Initialization
     */
    static
    {
        conf = HBaseConfiguration.create();
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    }

    /**
     * Create a table
     */
    public static void createTable(String tableName, String[] families) throws Exception
    {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(tableName))
        {
            System.out.println("table already exists!");
        }
        else
        {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < families.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(families[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }

    /**
     * Delete a table
     */
    public static void deleteTable(String tableName) throws Exception
    {
        try
        {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        }
        catch (MasterNotRunningException e)
        {
            e.printStackTrace();
        }
        catch (ZooKeeperConnectionException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Put (or insert) a row
     */
    public static void addRecord(String tableName, String rowKey,
                                 String family, String qualifier, String value) throws Exception
    {
        try
        {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("Insert record for index " + rowKey + " to table " + tableName + " : ok.");
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Delete a row
     */
    public static void delRecord(String tableName, String rowKey)
            throws IOException
    {
        HTable table = new HTable(conf, tableName);
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("Deletion record for index " + rowKey + "  : ok.");
    }

    /**
     * Get a row
     */
    public static void getOneRecord (String tableName, String rowKey) throws IOException
    {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }
    }

    /**
     * Scan (or list) a table
     */
    public static void getAllRecords (String tableName)
    {
        try
        {
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for(Result r:ss){
                for(KeyValue kv : r.raw()){
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.println(new String(kv.getValue()));
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Guiding outputs to add a new buddy
     */
    public static void newBuddy(String tableName) throws Exception
    {
        Scanner scan = new Scanner(System.in);
        String fName, bff="", age, mail, address;
        String friends;

        System.out.println("Enter name : ");
        fName = scan.nextLine();

        System.out.println("Enter age : ");
        age = scan.nextLine();

        System.out.println("Enter e-mail : ");
        mail = scan.nextLine();

        System.out.println("Enter address : ");
        address = scan.nextLine();

        while(bff.equals(""))
        {
            System.out.println("Enter the name of your best bud (cannot be empty) : ");
            bff = scan.nextLine();
        }

        System.out.println("Enter other friends separated by ';' ");
        friends = scan.nextLine();

        addRecord(tableName, fName, "info", "age", age);
        addRecord(tableName, fName, "info", "mail", mail);
        addRecord(tableName, fName, "info", "address", address);
        addRecord(tableName, fName, "friends", "bff", bff);
        addRecord(tableName, fName, "friends", "others", friends);

        System.out.println("Saved "+fName+" into database");
    }


    public static void main(String args[])
    {
        Scanner myScan = new Scanner(System.in);
        String input;
        try
        {
            String tableName = "gkleitz";
            String[] families = {"info", "friends"};
            hbase1.createTable(tableName, families);

            System.out.println("--YOKOSO--");
            System.out.println("Available inputs : 'see', 'add', 'exit'.");
            while(true)
            {
                System.out.print("¤");
                input = myScan.nextLine();

                if(input.equals("see"))
                {
                    hbase1.getAllRecords(tableName);
                }
                else if(input.equals("add"))
                {
                    hbase1.newBuddy(tableName);
                }
                else if(input.equals("exit"))
                {
                    return;
                }
                else
                {
                    System.out.println("Available inputs : 'see', 'add', 'exit'.");
                }
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}