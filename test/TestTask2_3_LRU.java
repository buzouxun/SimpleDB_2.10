import static org.junit.Assert.assertEquals;

import java.util.List;

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

		// initialize a new buffer manager which has 10 empty buffers indexing from 0 to 11
		int size_of_mt_buffers = 8;
		SimpleDB.setBm(new BufferMgr(size_of_mt_buffers, Policy.LRU));
		SimpleDB.myMetaData.cleanUp();

		// create students table
		TestUtility.exec_crt_tbl(TestUtility.get_crt_tbl_students(), SimpleDB.planner());

		// insert student records for querying later
		List<String> recs = TestUtility.get_values_persons();
		TestUtility.exec_insert_values(recs.subList(0, 3), SimpleDB.planner());
		TestUtility.exec_insert_values(recs.subList(0, 3), SimpleDB.planner());
		TestUtility.exec_insert_values(recs.subList(0, 3), SimpleDB.planner());

		// empty buffers have already occupied by creating table and inserting values
		assertEquals(size_of_mt_buffers - 1, SimpleDB.myMetaData.getLastUsedMtFrmIndex());

		// get a list of sorted buffers by logSequenceNumber
		List<String> buffer_ids = SimpleDB.bufferMgr().get_list_buffers_by_LRU();
		
		// start to record which buffer(s) is selected by LRU
		SimpleDB.myMetaData.LRUBufferIdLogCleanUp();
		
		// create students table
		TestUtility.exec_crt_tbl(TestUtility.get_crt_tbl_computers(), SimpleDB.planner());
		
		// confirm buffers selected by LRU MUST have oldest time stamp
		for(int i = 0; i < SimpleDB.myMetaData.LRUBufferIdLogs.size(); i++) {
			assertEquals( ((int)Integer.parseInt(buffer_ids.get(i).split(",")[0])), ((int)SimpleDB.myMetaData.LRUBufferIdLogs.get(i)) );
		}

		
		// print list buffers (id, logSequenceNumber)
		String printout = "\n*** List of buffers (id, logSequenceNumber) ***\n";
		for( int i = 0; i < SimpleDB.bufferMgr().get_list_buffers_ids_loqSeqNum().size(); i++) {
			printout += "id: " + SimpleDB.bufferMgr().get_list_buffers_ids_loqSeqNum().get(i).split(",")[0] 
					+ "\tlogSeqNum: " + SimpleDB.bufferMgr().get_list_buffers_ids_loqSeqNum().get(i).split(",")[1] + "\n";
		}
		
		// print list buffers sorted by logSequenceNumber
		printout += "\n*** List of buffers (id, logSequenceNumber) sorted by logSeqNum ***\n";
		for( int i = 0; i < buffer_ids.size(); i++) {
			printout += "id: " + buffer_ids.get(i).split(",")[0] 
					+ "\tlogSeqNum: " + buffer_ids.get(i).split(",")[1] + "\n";
		}
		
		// print buffers selected by LRU while creating computer table
		printout += "\n*** List of buffers (id, logSequenceNumber) selected by LRU after creating new table ***\n";
		for(int i = 0; i < SimpleDB.myMetaData.LRUBufferIdLogs.size(); i++) {
			printout += "id: " + SimpleDB.myMetaData.LRUBufferIdLogs.get(i) + "\n";
		}
		
		System.out.println(printout);
		
	}
}
