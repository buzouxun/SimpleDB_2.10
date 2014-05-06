package project2;

/******************************************************************/

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import project1.TestUtility;

import simpledb.remote.SimpleDriver;
import simpledb.server.Startup;

public class CreateTestTables {
	final static int maxSize = 20000;
	final static int rand_maxValue = 1000;
	static int num777 = 0;

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// Delete the files in user directory
		String user_home_path = System.getProperty("user.home").replace("\\", "/");
		String directory_path = user_home_path + "/studentdb";
		try {
			TestUtility.delete(new File(directory_path));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Initialize StudentDB
		String[] argument = {"studentdb"};
		Startup.main(argument);
		
		// Connect to the server
		Connection conn = null;
		Driver d = new SimpleDriver();
		String host = "localhost";	// you may change it if your SimpleDB server is running on a different machine
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
			s.executeUpdate("Create table test5" + "( a3 int," + "  a4 int" + ")");

			// create 3 index
			s.executeUpdate("create sh index idx1 on test1 (a1)");
			s.executeUpdate("create eh index idx2 on test2 (a1)");
			s.executeUpdate("create bt index idx3 on test3 (a1)");

			// insert values
			for (int i = 1; i <= 4; i++) {
				rand = new Random(1);	// ensure every table gets the same data
				for (int j = 0; j < maxSize; j++) {
					s.executeUpdate("insert into test" + i + " (a1,a2) values(" + rand.nextInt(rand_maxValue) + ","
							+ rand.nextInt(rand_maxValue) + ")");
				}
			}
			
			// insert values for test5
			rand = new Random(1);
			for (int k = 0; k < maxSize / 2; k++) {
				s.executeUpdate("insert into test5" + " (a3,a4) values(" + rand.nextInt(rand_maxValue) + ","
						+ rand.nextInt(rand_maxValue) + ")");
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
		
		// Run testing scenarios of selection and join processing
		String[] nullArg = {null};
		SelectTestTables.main(nullArg);
		JoinTestTables.main(nullArg);
		
		// Exit SimpleDB
		System.exit(0);
	}
}
