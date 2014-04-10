import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import simpledb.planner.Planner;
import simpledb.tx.Transaction;

public class TestUtility {

	@SuppressWarnings("resource")
	public static String get_crt_tbl_students() {
		String students_tbl = "";
		String  thisLine = null;
		FileReader file = null;
		try{
			file = new FileReader(System.getProperty("user.dir") + "/test/sql_examples/create_students.sql");
			BufferedReader reader = new BufferedReader(file);
			// check new line
			if ((thisLine = reader.readLine()) != null) {
				students_tbl = thisLine;
			}       
		}catch(Exception e){
			e.printStackTrace();
		}
		return students_tbl;
	}

	@SuppressWarnings("resource")
	public static List<String> get_values_persons() {
		List<String> students_values = new ArrayList<String>();
		String  thisLine = null;
		FileReader file = null;
		try{
			file = new FileReader(System.getProperty("user.dir") + "/test/sql_examples/insert_students.sql");
			BufferedReader reader = new BufferedReader(file);
			// check new line
			while ((thisLine = reader.readLine()) != null) {
				students_values.add(thisLine);
			}       
		}catch(Exception e){
			e.printStackTrace();
		}
		return students_values;
	}

	public static int exec_crt_tbl(String crt_tbl_persons, Planner planner) {
		int numRecs = 0;
		Transaction tx = new Transaction();
		numRecs = planner.executeUpdate(crt_tbl_persons, tx);
		tx.commit();
		return numRecs;
	}

	public static int exec_insert_values(List<String> students_values, Planner planner) {
		int sumRecs = 0;
		Transaction tx = null;
		for(int i = 0; i < students_values.size(); i++) {
			tx = new Transaction();
			sumRecs += planner.executeUpdate(students_values.get(i), tx);
			tx.commit();
		}
		return sumRecs;
	}
}	
