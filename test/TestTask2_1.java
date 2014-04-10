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

	@Test
	public void test_empty_frames_while_insert_values_into_students_table() {
		// analogous to the driver
		SimpleDB.init("studentdb");
		// create students table
		TestUtility.exec_crt_tbl(TestUtility.get_crt_tbl_students(), SimpleDB.planner());

		// initialize a new buffer manager which had 12 empty frames indexing from 0 to 1
		int size_of_mt_buffers = 12;
		SimpleDB.setBm(new BufferMgr(size_of_mt_buffers) );
		// clean up SimpleDB.myMetaData
		SimpleDB.myMetaData.cleanUp();

		// the first insertions of 6 records will not take all empty frames, since the size of relational data is not big enough
		List<String> recs = TestUtility.get_values_persons();
		TestUtility.exec_insert_values(recs.subList(0, 6), SimpleDB.planner());
		SimpleDB.myMetaData.printMtFrmLogs();
		assertEquals(true, SimpleDB.myMetaData.getLastUsedMtFrmIndex() < (size_of_mt_buffers - 1) );
		
		// after incrementally inserting another 6 records, the size of relational data is big enough to take all empty blocks
		TestUtility.exec_insert_values(recs.subList(0, 6), SimpleDB.planner());
		SimpleDB.myMetaData.printMtFrmLogs();
		assertEquals(true, SimpleDB.myMetaData.getLastUsedMtFrmIndex() == (size_of_mt_buffers - 1) );
	}

}
