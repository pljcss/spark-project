package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseDemo {

    /**
     * 声明静态配置，配置zookeeper
     */
    static Configuration configuration = null;
    static Connection connection = null;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "10.0.0.184:2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {
        queryAll("smartcommunity");
//        queryByRowId("smartcommunity", "\\x7F\\xFF\\xFE\\x9D\\xD6y\\xEE1");
    }


    /**
     * 查询所有数据
     */
//    public static void queryAll1(String tableName) throws Exception {
//        Table table = connection.getTable(TableName.valueOf(tableName));
//        try {
//            ResultScanner rs = table.getScanner(new Scan());
//            for (Result r : rs) {
//                System.out.println(r.getRow());
////                System.out.println("获得到rowkey:" + new String(r.getRow()));
////                for (Cell keyValue : r.rawCells()) {
////                    System.out.println("列：" + Bytes.toString(CellUtil.cloneFamily(keyValue))+":" + Bytes.toString(CellUtil.cloneQualifier(keyValue)) + "====值:" + Bytes.toString(CellUtil.cloneValue(keyValue)));
////                }
//            }
//            rs.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }



    /**
     * 根据rowId查询
     *
     * @param tableName
     * @throws Exception
     */
    public static void queryByRowId(String tableName, String rowId) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            // 根据rowkey查询
            Get scan = new Get(rowId.getBytes());
            Result r = table.get(scan);
//            System.out.println("获得到rowkey:" + new String(r.getRow()));
            System.out.println("获得到rowkey:" + Bytes.toString(r.getRow()));
            for (Cell keyValue : r.rawCells()) {
                System.out.println("列：" + new String(CellUtil.cloneFamily(keyValue)) + ":" + new String(CellUtil.cloneQualifier(keyValue)) + "====值:" + new String(CellUtil.cloneValue(keyValue)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * 查询所有数据
     */
    public static void queryAll(String tableName) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName.getBytes()));
        try {
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + Bytes.toString(r.getRow()));
                for (Cell keyValue : r.rawCells()) {
                    System.out.println("列：" + new String(CellUtil.cloneFamily(keyValue))+":"+new String(CellUtil.cloneQualifier(keyValue)) + "====值:" + new String(CellUtil.cloneValue(keyValue)));
                }
            }
            rs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
