import org.junit.Before;
import org.junit.Test;

import simpledb.buffer.BufferMgr;
import simpledb.buffer.BufferMgr.Policy;
import simpledb.server.SimpleDB;


public class TestTask2_3_LRU {
	@Before
	public void delete_students_tbl() {
		String user_home_path = System.getProperty("user.home").replace("\\", "/");
		TestUtility.JavaDeleteFile(user_home_path + "/studentdb/students.tbl");
		TestUtility.JavaDeleteFile(user_home_path + "/studentdb/fldcat.tbl");
		TestUtility.JavaDeleteFile(user_home_path + "/studentdb/tblcat.tbl");
		TestUtility.JavaDeleteFile(user_home_path + "/studentdb/simpledb.log");
	}
	
	@Test
	public void test_LRU() {
		// analogous to the driver
		SimpleDB.init("studentdb");
		// create students table
		TestUtility.exec_crt_tbl(TestUtility.get_crt_tbl_students(),
				SimpleDB.planner());

		// initialize a new buffer manager which has 10 empty buffers indexing
		// from 0 to 11
		int size_of_mt_buffers = 10;
		SimpleDB.setBm(new BufferMgr(size_of_mt_buffers, Policy.LRU));
		// clean up SimpleDB.myMetaData
		SimpleDB.myMetaData.cleanUp();
		
		
	}
}
