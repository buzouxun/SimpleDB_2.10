import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import simpledb.tx.Transaction;
import simpledb.buffer.BufferMgr;
import simpledb.query.*;
import simpledb.server.SimpleDB;


/**
 * 
 */

/**
 * @author jzhu
 *
 */
public class TestTask2_1 {

	/*
	@Test
	public void exec_crt_tbl_persons() {
		// analogous to the driver
		SimpleDB.init("studentdb");	
		// create students table
		int numAffRecs = TestUtility.exec_crt_tbl(TestUtility.get_crt_tbl_students(), SimpleDB.planner());
		assertEquals(0, numAffRecs);
	}
	
	@Test
	public void exec_insert_values_persons() {
		// analogous to the driver
		SimpleDB.init("studentdb");
		// create students table
		TestUtility.exec_crt_tbl(TestUtility.get_crt_tbl_students(), SimpleDB.planner());	
		// insert values of students
		int numAffRecs = TestUtility.exec_insert_values(TestUtility.get_values_persons(), SimpleDB.planner());
		assertEquals(3, numAffRecs);
	}
	
	*/

	@Test
	public void test_empty_frames_while_insert_values_into_students_table() {
		// analogous to the driver
		SimpleDB.init("studentdb");
		
		/*
		
		// create students table
		TestUtility.exec_crt_tbl(TestUtility.get_crt_tbl_students(), SimpleDB.planner());

		// initialize a new buffer manager which had 40 empty frames indexing from 0 to 39
		SimpleDB.setBm(new BufferMgr(40) );
		// clean up SimpleDB.myMetaData
		SimpleDB.myMetaData.cleanUp();
		
		// get student records to be inserted
		List<String> recs = TestUtility.get_values_persons();
		// insert the first student record
		TestUtility.exec_insert_values(recs.subList(0, 1), SimpleDB.planner());
		assertEquals(15, SimpleDB.myMetaData.getLastUsedMtFrmIndex() );
		// insert the second student record
		TestUtility.exec_insert_values(recs.subList(1, 2), SimpleDB.planner());
		assertEquals(31, SimpleDB.myMetaData.getLastUsedMtFrmIndex() );
		// insert the third student record
		TestUtility.exec_insert_values(recs.subList(2, 3), SimpleDB.planner());
		assertEquals(39, SimpleDB.myMetaData.getLastUsedMtFrmIndex() );
		
		*/
		
	}

}
