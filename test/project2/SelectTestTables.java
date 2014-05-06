package project2;

/******************************************************************/

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;

import project1.TestUtility;
import simpledb.remote.SimpleDriver;
import simpledb.server.Startup;

public class SelectTestTables {
	static long startDateTime = 0;
	static long endDateTime = 0;
	static int equalityValue = 896;

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// Connect to the server
		Connection conn = null;
		Driver d = new SimpleDriver();
		String host = "localhost";	// you may change it if your SimpleDB server is running on a different machine
		String url = "jdbc:simpledb://" + host;
		Statement s = null;
		try {
			conn = d.connect(url, null);
			s = conn.createStatement();
			
			// 4 selection query			
			String unit = "\tnano second";
			
			// test1
			markStartTime();
			s.executeQuery("SELECT a1,a2 FROM test1 where a1=" + equalityValue + ";");
			markEndTime();
			System.out.println("Selection time of test1:\t" + cousumedTime() + unit);
			
			// test2
			markStartTime();
			s.executeQuery("SELECT a1,a2 FROM test2 where a1=" + equalityValue + ";");
			markEndTime();
			System.out.println("Selection time of test2:\t" + cousumedTime() + unit);
			
			// test3
			markStartTime();
			s.executeQuery("SELECT a1,a2 FROM test3 where a1=" + equalityValue + ";");
			markEndTime();
			System.out.println("Selection time of test3:\t" + cousumedTime() + unit);
			
			// test4
			markStartTime();
			s.executeQuery("SELECT a1,a2 FROM test4 where a1=" + equalityValue + ";");
			markEndTime();
			System.out.println("Selection time of test4:\t" + cousumedTime() + unit);
			
			conn.close();

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * mark the current Date().time stored in global variable, startDateTime
	 */
	public static void markStartTime() {
		startDateTime = System.nanoTime();
	}
	
	/**
	 * mark the current Date().time stored in global variable, endDateTime
	 */
	public static void markEndTime() {
		endDateTime = System.nanoTime();
	}
	
	/**
	 * return the consumed time between start time and end time
	 * @return
	 */
	public static long cousumedTime() {
		return endDateTime - startDateTime;
	}
	
}
