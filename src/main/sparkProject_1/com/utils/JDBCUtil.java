package com.utils;

import java.io.*;
import java.sql.*;
import java.util.Properties;

/**
 * JDBC工具类 : 将对数据库的通用的操作封装起来
 * @author luds
 */
public class JDBCUtil {
	
	
	private static Properties p = new Properties();
	
	static {
        // 将驱动的注册, 放到这里执行
		try {
            // 将配置文件中的所有的数据都读取到的p中
			p.load(new BufferedReader(new FileReader("source/source.properties")));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

        // 注册驱动
		try {
			Class.forName(p.getProperty("driver"));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

    /**
     * 可以直接通过这个方法获取一个连接对象
     * @return
     */
	public static Connection getConnection() {
		
		// 1. ��������
		String host = p.getProperty("host");
		String user = p.getProperty("user");
		String password = p.getProperty("passwd");
		
		try {
			return DriverManager.getConnection(host, user, password);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

    /**
     * 释放资源
     * @param st
     * @param conn
     */
	public static void close(Statement st, Connection conn) {
		close(null, st, conn);
	}

    /**
     * 释放资源
     * @param set
     * @param st
     * @param conn
     */
	public static void close(ResultSet set, Statement st, Connection conn) {
		if (set != null) {
			try {
				set.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (st != null) {
			try {
				st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
}
