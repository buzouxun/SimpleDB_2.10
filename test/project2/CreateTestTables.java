package project2;

/******************************************************************/

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import simpledb.index.exthash.ExtHashIndex;
import simpledb.record.Schema;
import simpledb.remote.SimpleDriver;
import simpledb.tx.Transaction;

public class CreateTestTables {
	final static int maxSize = 466;
	final static int rand_maxValue = 1000;
	static int num777 = 0;

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
			for (int i = 2; i < 3; i++) {
				if (i != 5) {
					rand = new Random(1);// ensure every table gets the same data
					for (int j = 0; j < maxSize; j++) {
						
						//TODO print j
						System.out.println("\n****\njj+ " + j + "\n****\n");
						int nextRandInt = rand.nextInt(rand_maxValue);
						rand.nextInt(rand_maxValue);
						
						s.executeUpdate("insert into test" + i
//								+ " (a1,a2) values(" + rand.nextInt(rand_maxValue) + "," + rand.nextInt(rand_maxValue) + ")");
								+ " (a1,a2) values(" + nextRandInt + "," + nextRandInt + ")");
//								+ " (a1,a2) values(" + j + "," + j + ")");
//								+ " (a1,a2) values(" + 0 + "," + 0 + ")");
						
						
//						if(nextRandInt == 165) {
//							num777 ++;
//						}
					}
				} else// case where i=5
				{
//					for (int j = 0; j < maxSize / 2; j++) // insert 10000 records into test5
					for (int j = 0; j < maxSize; j++) // insert same number of records
					{
						s.executeUpdate("insert into test" + i
								+ " (a3,a4) values(" + j + "," + j + ")");
					}
				}
			}
			
			//TODO insert a value whose second hash index  = 9
//			for(int k = 0; k < 26; k++) {
//				s.executeUpdate("insert into test" + 2 + " (a1,a2) values(" + 9 + "," + 9 + ")");
//			}
			
//			System.out.println("num777 = " + num777);
			
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
