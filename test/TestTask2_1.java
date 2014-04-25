import static org.junit.Assert.*;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import simpledb.buffer.BufferMgr;
import simpledb.server.SimpleDB;


/**
 * 
 */

/**
 * @author jzhu
 *
 */
public class TestTask2_1 {
	
	@Before
	public void delete_students_tbl() {
		String user_home_path = System.getProperty("user.home").replace("\\", "/");
		TestUtility.JavaDeleteFile(user_home_path + "/studentdb/students.tbl");
		TestUtility.JavaDeleteFile(user_home_path + "/studentdb/fldcat.tbl");
		TestUtility.JavaDeleteFile(user_home_path + "/studentdb/tblcat.tbl");
		TestUtility.JavaDeleteFile(user_home_path + "/studentdb/simpledb.log");
	}

	@Test
	public void test_empty_frames_while_insert_values_into_students_table() {
		
		// description to be printed
		String printout = "\n";
		
		// analogous to the driver
		SimpleDB.init("studentdb");
		// create students table
		TestUtility.exec_crt_tbl(TestUtility.get_crt_tbl_students(), SimpleDB.planner());

		// initialize a new buffer manager which has 12 empty buffers indexing from 0 to 11
		int size_of_mt_buffers = 12;
		SimpleDB.setBm(new BufferMgr(size_of_mt_buffers) );
		SimpleDB.myMetaData.cleanUp();
		printout += "new buffer manager initilized 12 empty buffers, index from 0 to 11, waiting for inserting values\n\n";

		// the first insertions of 6 records will not take all empty buffers, since the size of relational data is not big enough
		List<String> recs = TestUtility.get_values_persons();
		TestUtility.exec_insert_values(recs.subList(0, 6), SimpleDB.planner());
		assertEquals(true, SimpleDB.myMetaData.getLastUsedMtFrmIndex() < (size_of_mt_buffers - 1) );
		printout += "After inserting 6 records, the last occupied empty buffer id: " + SimpleDB.myMetaData.getLastUsedMtFrmIndex() + "\n" 
				+ "In other words, buffer 0 ~ 7 have been occupied. \n\n";
		
		// after incrementally inserting another 6 records, the size of relational data is big enough to take all empty blocks
		TestUtility.exec_insert_values(recs.subList(0, 6), SimpleDB.planner());
		assertEquals(true, SimpleDB.myMetaData.getLastUsedMtFrmIndex() == (size_of_mt_buffers - 1) );
		printout += "After inserting another 6 records, the last occupied empty buffer id: " + SimpleDB.myMetaData.getLastUsedMtFrmIndex() + "\n"
				+ "In other words, all empty buffers have been occupied.\n";
	
		System.out.println(printout);
	}
	
	

}