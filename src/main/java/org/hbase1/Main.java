package org.hbase1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static final String TABLE_NAME="users";
    public static final String CF_PERSONAL_DATA="personal_daya";
    public static final String CF_PROFESSIONAL_DATA="professional_daya";
    public static void display(Result result) {
        for(Cell cell: result.rawCells()) {
            byte[] cf = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);

            String cfString = Bytes.toString(cf);
            String qualifierString = Bytes.toString(qualifier);
            String valueString = Bytes.toString(value);

            System.out.println("Column Family: " + cfString +
                    ", Column: " + qualifierString +
                    ", Value: " + valueString);
        }
    }
    public static void main(String[] args) {
        Configuration configuration= HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","zookeeper");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        configuration.set("hbase.master","hbase-master:1600");
        try {
            Connection connection= ConnectionFactory.createConnection(configuration);
            Admin admin=connection.getAdmin();

            TableName tableName=TableName.valueOf(TABLE_NAME);
            TableDescriptorBuilder builder=TableDescriptorBuilder.newBuilder(tableName);
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_PERSONAL_DATA));
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_PROFESSIONAL_DATA));
            TableDescriptor tableDescriptor=builder.build();

            if (!admin.tableExists(tableName)){
                admin.createTable(tableDescriptor);
            }else {
                System.out.println("la table est déjà créée");
            }

            Table table=connection.getTable(tableName);
            Put put=new Put(Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL_DATA),Bytes.toBytes("name"),Bytes.toBytes("KHAoula l jamila"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL_DATA),Bytes.toBytes("age"),Bytes.toBytes("22"));
            put.addColumn(Bytes.toBytes(CF_PROFESSIONAL_DATA),Bytes.toBytes("diplome"),Bytes.toBytes("ingénieur"));
            table.put(put);
            System.out.println("La ligne a été bien insérée");

            Get get=new Get(Bytes.toBytes("11111"));
            Result result=table.get(get);

            display(result);

            put = new Put(Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL_DATA), Bytes.toBytes("name"), Bytes.toBytes("Reda"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL_DATA), Bytes.toBytes("age"), Bytes.toBytes("22"));
            put.addColumn(Bytes.toBytes(CF_PROFESSIONAL_DATA), Bytes.toBytes("dip"), Bytes.toBytes("BDCC ing"));
            table.put(put);
            System.out.println("The record has been updated !");


            get = new Get(Bytes.toBytes("1"));
            result = table.get(get);

            display(result);

            byte[] name=result.getValue(Bytes.toBytes(CF_PERSONAL_DATA),Bytes.toBytes("name"));
            System.out.println(Bytes.toString(name));

            Delete delete=new Delete(Bytes.toBytes("11111"));
            table.delete(delete);
            System.out.println("Données supprimées");

            admin.disableTable(tableName);
            admin.deleteTable(tableName);

            System.out.println("The table has been deleted !");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}