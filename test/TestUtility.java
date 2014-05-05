import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import simpledb.planner.Planner;
import simpledb.query.Plan;
import simpledb.tx.Transaction;


/**
 * cs4432-Project1: Test Utility function class
 * 
 * @author jzhu
 *
 */
public class TestUtility {

	/**
	 * create table of students.sql
	 * @return
	 */
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

	
	/**
	 * get values/records to be inserted into students table
	 * @return
	 */
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
	
	
	/**
	 * select names from students relational table
	 * @return
	 */
	@SuppressWarnings("resource")
	public static String get_select_name_of_students() {
		String query = "";
		String  thisLine = null;
		FileReader file = null;
		try{
			file = new FileReader(System.getProperty("user.dir") + "/test/sql_examples/select_students.sql");
			BufferedReader reader = new BufferedReader(file);
			// check new line
			if ((thisLine = reader.readLine()) != null) {
				query = thisLine;
			}       
		}catch(Exception e){
			e.printStackTrace();
		}
		return query;
	}

	
	/**
	 * execute the create table statements
	 * @param crt_tbl
	 * @param planner
	 * @return
	 */
	public static int exec_crt_tbl(String crt_tbl, Planner planner) {
		int numRecs = 0;
		Transaction tx = new Transaction();
		numRecs = planner.executeUpdate(crt_tbl, tx);
		tx.commit();
		return numRecs;
	}

	
	/**
	 * execute the insert statement
	 * @param students_values
	 * @param planner
	 * @return
	 */
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
	
	
	/**
	 * execute select statements
	 * @param query
	 * @param planner
	 * @return
	 */
	public static Plan exec_select(String query, Planner planner) {
		Transaction tx = new Transaction();
		Plan p = planner.createQueryPlan(query, tx);
		tx.commit();
		return p;
	}

	
	/**
	 * create table of students.sql
	 * @return
	 */
	@SuppressWarnings("resource")
	public static String get_crt_tbl_computers() {
		String students_tbl = "";
		String  thisLine = null;
		FileReader file = null;
		try{
			file = new FileReader(System.getProperty("user.dir") + "/test/sql_examples/create_computers.sql");
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
	
	
	
	
	
	
	
	
	
	/**
	 * delete the existing tables for testing purpose
	 * @param file
	 */
	public static void JavaDeleteFile(String file) {
		try
		{
			//this is a file in the current, working directory

			//you can use absolute path here as well

			//for example C://Java/myfile.dat
			File f = new File(file);

			if(f.exists() && f.isFile()){
				boolean success = f.delete();
				if (success) {
					System.out.println("File " + f.getPath() + " has been deleted");
				} else {
					System.err.println("File " + f.getPath() + " has not been deleted");
				}
			} else {
				System.out.println("File " + f.getPath() + " does not exists or " +
						"it is a folder");
			}
		} catch (SecurityException e) {
			System.err.println("Deleting a file is denied!");
			e.printStackTrace();
		}
	}
	
	
	
	public static void delete(File file) {

		try {
		if(file.isDirectory()){

			//directory is empty, then delete it
			if(file.list().length==0){

				file.delete();
				System.out.println("Directory is deleted : " 
						+ file.getAbsolutePath());

			}else{

				//list all the directory contents
				String files[] = file.list();

				for (String temp : files) {
					//construct the file structure
					File fileDelete = new File(file, temp);

					//recursive delete
					delete(fileDelete);
				}

				//check the directory again, if empty then delete it
				if(file.list().length==0){
					file.delete();
					System.out.println("Directory is deleted : " 
							+ file.getAbsolutePath());
				}
			}

		}else{
			//if file, then delete it
			file.delete();
			System.out.println("File is deleted : " + file.getAbsolutePath());
		}
		}
		catch (IOException e) {
			System.err.println("Deleting studentdb directory is denied!");
			e.printStackTrace();
		}
	}
	

}	
