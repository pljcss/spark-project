package com.spark.streaming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

/**
 * 简易版的连接池
 */
public class ConnectionPool {

    /**
     * 静态的 Connection 队列
     */
    private static LinkedList<Connection> connectionQueue;

    /**
     * 加载驱动
     */
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接, 多线程访问并发控制
     * @return
     */
    public synchronized static Connection getConnections() {

        try {
            if (connectionQueue == null) {

                connectionQueue = new LinkedList<>();

                for (int i = 0; i < 10; i++) {
                    Connection conn = DriverManager.getConnection(
                            // 10.0.0.184数据库不允许这个ip访问, 所以需要使用localhost
                            "jdbc:mysql://localhost:3306/test",
                            "root",
                            "gt123"
                    );

                    connectionQueue.push(conn);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return connectionQueue.poll();
    }

    /**
     * 还回去一个连接
     */
    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }

}
