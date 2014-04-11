import static org.junit.Assert.*;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import simpledb.buffer.BufferMgr;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;

/**
 * 
 */

/**
 * @author jzhu
 *
 */
public class TestTask2_2 {

	@Before
	public void delete_students_tbl() {
		String user_home_path = System.getProperty("user.home").replace("\\", "/");
		TestUtility.JavaDeleteFile(user_home_path + "/studentdb/students.tbl");
		TestUtility.JavaDeleteFile(user_home_path + "/studentdb/fldcat.tbl");
		TestUtility.JavaDeleteFile(user_home_path + "/studentdb/tblcat.tbl");
		TestUtility.JavaDeleteFile(user_home_path + "/studentdb/simpledb.log");
	}

	@Test
	public void test_find_existing_buffers_while_running_same_query() {
		
		// description to be printed
		String printout = "\n";

		// analogous to the driver
		SimpleDB.init("studentdb");
		// create students table
		TestUtility.exec_crt_tbl(TestUtility.get_crt_tbl_students(), SimpleDB.planner());

		// initialize a new buffer manager which has 10 empty buffers indexing from 0 to 11
		int size_of_mt_buffers = 10;
		SimpleDB.setBm(new BufferMgr(size_of_mt_buffers));
		SimpleDB.myMetaData.cleanUp();
		printout += "Initialize a new buffer manager which has 10 empty buffers indexing from 0 to 9, waiting for querying later\n\n";

		// insert 2 student records for querying later
		List<String> recs = TestUtility.get_values_persons();
		TestUtility.exec_insert_values(recs.subList(0, 2), SimpleDB.planner());

		// Query 1: select one column values from student records
		Plan p1 = TestUtility.exec_select(TestUtility.get_select_name_of_students(), SimpleDB.planner());
		String p1_result = concat_results(p1);
		int p1_last_occupied_mt_buffer_index = SimpleDB.myMetaData.getLastUsedMtFrmIndex();
		printout += "After running Query 1, the last occupied empty buffer id: " + p1_last_occupied_mt_buffer_index + 
				"\nIn other words, there are still some empty buffers left." + "\n\n";


		// Query 2: select the same column values from student records again
		Plan p2 = TestUtility.exec_select(TestUtility.get_select_name_of_students(), SimpleDB.planner());
		String p2_result = concat_results(p2);
		int p2_last_occupied_mt_buffer_index = SimpleDB.myMetaData.getLastUsedMtFrmIndex();
		printout += "Query 2 is as the same as Query 1, so it will not require any new buffer while running Query 2"
				+ "\nAfter running Query 2, the last occupied empty buffer id: " + p2_last_occupied_mt_buffer_index
				+ ".\nIn other words, running Query 2 only needs to use existing buffers.";


		// compare the results from the two selection statements (Query 1 and Query 2) above, which should be the same
		assertEquals(true, p1_result.length() > 0);
		assertEquals(true, p2_result.length() > 0);
		assertEquals(true, p1_result.equals(p2_result));

		
		// confirm that query 2 did not occupy any empty buffers, but only used existing buffers
		assertEquals(true, p1_last_occupied_mt_buffer_index < size_of_mt_buffers -1);
		assertEquals(p1_last_occupied_mt_buffer_index, p2_last_occupied_mt_buffer_index);
		
		// print buffers /  toString()
		System.out.println(printout);
	}

	private String concat_results(Plan p1) {
		String results = "";
		Scan s = p1.open();
		while (s.next()) {
			String name = s.getString("name");
			results += name;
		}
		s.close();
		return results;
	}

}
