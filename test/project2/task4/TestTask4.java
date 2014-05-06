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
import simpledb.query.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import static org.junit.Assert.*;

public class TestTask4 {
	
	// variables
	static int numRecordToInsert = 20000;
	static long startDateTime = 0;
	static long endDateTime = 0;

	@Test
	public void create_tables() {
		// delete studentdb folder
		String user_home_path = System.getProperty("user.home").replace("\\", "/");
		String directory_path = user_home_path + "/studentdb";
		try {
			TestUtility.delete(new File(directory_path));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// analogous to the driver
		SimpleDB.init("studentdb");
		
		// create table test1 and insert values
		TestUtility.exec_crt_tbl("Create table test1" + "( a1 int," + "  a2 int" + ")", SimpleDB.planner());
		for(int i = 1; i <= numRecordToInsert; i++) {
			List<String> values = new ArrayList<String>();
			values.add( "insert into test1" + " (a1,a2) values(" + i + "," + i + ")" );
			TestUtility.exec_insert_values(values, SimpleDB.planner());
		}
		
		// create table test2 and insert values
		TestUtility.exec_crt_tbl("Create table test2" + "( b1 int," + "  b2 int" + ")", SimpleDB.planner());
		for(int i = numRecordToInsert; i >= 1; i--) {
			List<String> values = new ArrayList<String>();
			values.add( "insert into test2" + " (b1,b2) values(" + i + "," + i + ")" );
			TestUtility.exec_insert_values(values, SimpleDB.planner());
		}	
	}
	
	
	@Test
	public void first_join() {
		
		// analogous to the driver
		SimpleDB.init("studentdb");
				
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
		System.out.println("The first join took " + cousumedTime() + "\tnano seconds");
		
	}
	
	@Test
	public void second_join() {
		
		// analogous to the driver
		SimpleDB.init("studentdb");
				
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
		System.out.println("The second join took " + cousumedTime() + "\tnano seconds");
	}
	
	
	/**
	 * select all from test1 table
	 */
	@Test
	public void select_test1_table() {
		// analogous to the driver
		SimpleDB.init("studentdb");
		Plan p = TestUtility.exec_select("SELECT a1,a2 FROM test1", SimpleDB.planner());
		String p_result = concat_test1_results(p);
		System.out.println(p_result);
	}
	
	/**
	 * select all from test2 table
	 */
	@Test
	public void select_test2_table() {
		// analogous to the driver
		SimpleDB.init("studentdb");
		Plan p = TestUtility.exec_select("SELECT b1,b2 FROM test2", SimpleDB.planner());
		String p_result = concat_test2_results(p);
		System.out.println(p_result);
	}
	
	/**
	 * output test1 selection result
	 * @param p
	 * @return
	 */
	private String concat_test1_results(Plan p) {
		String results = "";
		// execute the plan
		markStartTime();
		Scan s = p.open();
		markEndTime();
		while (s.next()) {
			results += "a1= " + s.getInt("a1") + ", ";
			results += "a2= " + s.getInt("a2") + ", ";
			results += "\n";
		}
		s.close();
		return results;
	}
	
	/**
	 * output test2 selection result
	 * @param p
	 * @return
	 */
	private String concat_test2_results(Plan p) {
		String results = "";
		// execute the plan
		markStartTime();
		Scan s = p.open();
		markEndTime();
		while (s.next()) {
			results += "b1= " + s.getInt("b1") + ", ";
			results += "b2= " + s.getInt("b2") + ", ";
			results += "\n";
		}
		s.close();
		return results;
	}
	
	/**
	 * output join result
	 * @param p
	 * @return
	 */
	private String concat_results(Plan p) {
		String results = "";
		// execute the plan
		markStartTime();
		Scan s = p.open();
		markEndTime();
		while (s.next()) {
			try {
				results += "a1= " + s.getInt("a1") + ", ";
				results += "a2= " + s.getInt("a2") + ", ";
				results += "b1= " + s.getInt("b1") + ", ";
				results += "b2= " + s.getInt("b2") + ", ";
			} catch(Exception e) {
				e.printStackTrace();
			}
			results += "\n";
		}
		s.close();
		return results;
	}
	
	/**
	 * mark the current Date().time stored in global variable, startDateTime
	 */
	public static void markStartTime() {
		startDateTime = System.nanoTime();
	}
	
	/**
	 * mark the current Date().time stored in global variable, endDateTime
	 */
	public static void markEndTime() {
		endDateTime = System.nanoTime();
	}
	
	/**
	 * return the consumed time between start time and end time
	 * @return
	 */
	public static long cousumedTime() {
		return endDateTime - startDateTime;
	}
}
