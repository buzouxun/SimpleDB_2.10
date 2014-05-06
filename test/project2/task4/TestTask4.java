package project2.task4;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import simpledb.buffer.BufferMgr;
import simpledb.index.planner.IndexUpdatePlanner;
import simpledb.opt.ExploitSortQueryPlanner;
import simpledb.opt.HeuristicQueryPlanner;
import simpledb.parse.Parser;
import simpledb.parse.QueryData;
import simpledb.planner.BasicQueryPlanner;
import simpledb.planner.Planner;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import static org.junit.Assert.*;

public class TestTask4 {

	@Before
	public void delete_students_tbl() {
		String user_home_path = System.getProperty("user.home").replace("\\", "/");
		String directory_path = user_home_path + "/studentdb";
		try {
			TestUtility.delete(new File(directory_path));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test_empty_frames_while_insert_values_into_students_table() {
		// variables
		int numRecordToInsert = 2;
		
		// analogous to the driver
		SimpleDB.proj2_task5 = true;
		SimpleDB.init("studentdb");
		
		// create table test1 and insert values
		TestUtility.exec_crt_tbl("Create table test1" + "( a1 int," + "  a2 int" + ")", SimpleDB.planner());
		for(int i = 1; i <= numRecordToInsert; i++) {
			List<String> values = new ArrayList<String>();
			values.add( "insert into test1" + " (a1,a2) values(" + i + "," + i + ")" );
//			Planner planner = new Planner(new HeuristicQueryPlanner(), new IndexUpdatePlanner());
//			TestUtility.exec_insert_values(values, planner);
			TestUtility.exec_insert_values(values, SimpleDB.planner());
		}
		
		// create table test2 and insert values
		TestUtility.exec_crt_tbl("Create table test2" + "( b1 int," + "  b2 int" + ")", SimpleDB.planner());
		for(int i = 1; i <= numRecordToInsert; i++) {
			List<String> values = new ArrayList<String>();
			values.add( "insert into test2" + " (b1,b2) values(" + i*2 + "," + i*2 + ")" );
			TestUtility.exec_insert_values(values, SimpleDB.planner());
		}
		
		// join test1 and test2 where test1.a1 = test2.b1
		// create a new transaction for mergejoin job
		Transaction mjTx = new Transaction();
		// create query string for test1 and test2
		String test1_qry = "SELECT a1,a2 FROM test1";
		String test2_qry = "SELECT b1,b2 FROM test2";
		// create equality fldname for test1 and test2
		String test1_fldname = "a1";
		String test2_fldname = "b1";
		// create QueryDate for test1 and test2
		QueryData test1_qData = new Parser(test1_qry).query();
		QueryData test2_qData = new Parser(test2_qry).query();
		// create Plan (ProjectPlan) for test1 and test2
		Plan test1_plan = new BasicQueryPlanner().createPlan(test1_qData, mjTx);
		Plan test2_plan = new BasicQueryPlanner().createPlan(test2_qData, mjTx);
		// doing merge join
		Plan p = new ExploitSortQueryPlanner().createPlan(test1_plan, test2_plan, test1_fldname, test2_fldname, mjTx);
		// print results
		String p1_result = concat_results(p);
		System.out.println("p1_result: " + p1_result);
		
		
		
		
		
		// naive way of join
//		Plan p1 = TestUtility.exec_select("SELECT a1,a2,b1,b2 FROM test1, test2 WHERE a1=b1;", SimpleDB.planner());
//		String p1_result = concat_results(p1);
//		System.out.println("p1_result: " + p1_result);
		
	}
	
	private String concat_results(Plan p) {
		String results = "";
		// execute the plan
		Scan s = p.open();
		while (s.next()) {
			results += "a1= " + s.getInt("a1") + ", ";
			results += "a2= " + s.getInt("a2") + ", ";
			results += "b1= " + s.getInt("b1") + ", ";
			results += "b2= " + s.getInt("b2") + ", ";
			results += "\n";
		}
		s.close();
		return results;
	}
}
