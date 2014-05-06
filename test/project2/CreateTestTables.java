package project2;

/******************************************************************/

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import simpledb.remote.SimpleDriver;

public class CreateTestTables {
	final static int maxSize = 1000;
	final static int rand_maxValue = 100;

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Connection conn = null;
		Driver d = new SimpleDriver();
		String host = "localhost"; // you may change it if your SimpleDB server is running on a different machine
		String url = "jdbc:simpledb://" + host;
		Random rand = null;
		Statement s = null;
		try {
			conn = d.connect(url, null);
			s = conn.createStatement();

			// create 5 tables
			s.executeUpdate("Create table test1" + "( a1 int," + "  a2 int" + ")");
			s.executeUpdate("Create table test2" + "( a1 int," + "  a2 int" + ")");
			s.executeUpdate("Create table test3" + "( a1 int," + "  a2 int" + ")");
			s.executeUpdate("Create table test4" + "( a1 int," + "  a2 int" + ")");
			s.executeUpdate("Create table test5" + "( a1 int," + "  a2 int" + ")");

			// create 4 index
			s.executeUpdate("create sh index idx1 on test1 (a1)");
			s.executeUpdate("create eh index idx2 on test2 (a1)");
			
//			s.executeUpdate("create sh index idx2 on test2 (a1)");		//TODO testing
			
			s.executeUpdate("create bt index idx3 on test3 (a1)");

			// insert values
			for (int i = 1; i < 6; i++) {
				if (i != 5) {
					rand = new Random(1);// ensure every table gets the same data
					for (int j = 0; j < maxSize; j++) {
						s.executeUpdate("insert into test" + i
								+ " (a1,a2) values(" + rand.nextInt(rand_maxValue) + ","
								+ rand.nextInt(rand_maxValue) + ")");
					}
				} else// case where i=5
				{
//					for (int j = 0; j < maxSize / 2; j++) // insert 10000 records into test5
					for (int j = 0; j < maxSize / 2; j++) // insert same number of records
					{
						s.executeUpdate("insert into test" + i
								+ " (a3,a4) values(" + j + "," + j + ")");
					}
				}
			}

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
}
