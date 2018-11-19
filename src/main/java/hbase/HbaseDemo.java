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
        System.out.println("------");
        queryByRowId("smartcommunity", "ss");
    }


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
            System.out.println("rowkey: " + Bytes.toLong(r.getRow()));
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
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                /**
                 * 注意,rowkey插入时是Long类型,如果接收使用 Bytes.toString(r.getRow()) 或者
                 * new String(r.getRow())接收, 会出现乱码, 此处应该使用 Bytes.toLong(r.getRow()) 接收
                 */
                System.out.println("rowkey: " + Bytes.toLong(r.getRow()));
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
