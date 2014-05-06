package simpledb.server;

import java.util.ArrayList;
import java.util.List;

import simpledb.file.FileMgr;
import simpledb.buffer.*;
import simpledb.tx.Transaction;
import simpledb.log.LogMgr;
import simpledb.metadata.MetadataMgr;
import simpledb.planner.*;
import simpledb.opt.ExploitSortQueryPlanner;
import simpledb.opt.HeuristicQueryPlanner;
import simpledb.index.planner.IndexUpdatePlanner;

/**
 * The class that provides system-wide static global values.
 * These values must be initialized by the method
 * {@link #init(String) init} before use.
 * The methods {@link #initFileMgr(String) initFileMgr},
 * {@link #initFileAndLogMgr(String) initFileAndLogMgr},
 * {@link #initFileLogAndBufferMgr(String) initFileLogAndBufferMgr},
 * and {@link #initMetadataMgr(boolean, Transaction) initMetadataMgr}
 * provide limited initialization, and are useful for 
 * debugging purposes.
 * 
 * @author Edward Sciore
 */
public class SimpleDB {
   public static int BUFFER_SIZE = 16;
   public static String LOG_FILE = "simpledb.log";
   
   private static FileMgr     fm;
   private static BufferMgr   bm;
   private static LogMgr      logm;
   private static MetadataMgr mdm;
   
   /**
    * Initializes the system.
    * This method is called during system startup.
    * @param dirname the name of the database directory
    */
   public static void init(String dirname) {
      initFileLogAndBufferMgr(dirname);
      Transaction tx = new Transaction();
      boolean isnew = fm.isNew();
      if (isnew)
         System.out.println("creating new database");
      else {
         System.out.println("recovering existing database");
         tx.recover();
      }
      initMetadataMgr(isnew, tx);
      tx.commit();
   }
   
   // The following initialization methods are useful for 
   // testing the lower-level components of the system 
   // without having to initialize everything.
   
   /**
    * Initializes only the file manager.
    * @param dirname the name of the database directory
    */
   public static void initFileMgr(String dirname) {
      fm = new FileMgr(dirname);
   }
   
   /**
    * Initializes the file and log managers.
    * @param dirname the name of the database directory
    */
   public static void initFileAndLogMgr(String dirname) {
      initFileMgr(dirname);
      logm = new LogMgr(LOG_FILE);
   }
   
   /**
    * Initializes the file, log, and buffer managers.
    * @param dirname the name of the database directory
    */
   public static void initFileLogAndBufferMgr(String dirname) {
      initFileAndLogMgr(dirname);
      bm = new BufferMgr(BUFFER_SIZE);
   }
   
   /**
    * Initializes metadata manager.
    * @param isnew an indication of whether a new
    * database needs to be created.
    * @param tx the transaction performing the initialization
    */
   public static void initMetadataMgr(boolean isnew, Transaction tx) {
      mdm = new MetadataMgr(isnew, tx);
   }
   
   public static FileMgr     fileMgr()   { return fm; }
   public static BufferMgr   bufferMgr() { return bm; }
   public static LogMgr      logMgr()    { return logm; }
   public static MetadataMgr mdMgr()     { return mdm; }
   
   /**
    * Creates a planner for SQL commands.
    * To change how the planner works, modify this method.
    * @return the system's planner for SQL commands
    */
   public static Planner planner() {
	   QueryPlanner  qplanner = new HeuristicQueryPlanner();
	   UpdatePlanner uplanner = new IndexUpdatePlanner();
	   return new Planner(qplanner, uplanner);
   }

    /**
     * CS4432-Project1: setter for BufferMgr
     * @author jzhu1 & tli
     * @param bm the bm to set
     */
    public static void setBm(BufferMgr bm) {
    	SimpleDB.bm = bm;
    }
    
    /**
     * CS4432-Project1: Meta data class storing various logs for 
     * task2.1 - empty frames, task2.2 - efficient buffer search, 
     * task2.3 - list of buffers under LRU & Clock policies, and transactions
     * @author jzhu1 & tli
     */
    public static class myMetaData {
    	
    	private static List<String> txLogs = new ArrayList<String>();
    	private static int mtFrmIndex = 0;
    	private static List<Integer> mtFrmLogs = new ArrayList<Integer>();
    	public static List<Integer> LRUBufferIdLogs = new ArrayList<Integer>();
    	
    	/**
    	 * CS4432-Project1: add a log while a new transaction created or a transaction committed
    	 * @param newLog
    	 */
    	public static void addTxLog(String newLog) {
    		txLogs.add(newLog);
    	}
    	
    	/**
    	 * CS4432-Project1: print transaction logs
    	 */
    	public static void printTxLogs() {
    		for(int i = 0; i < txLogs.size(); i+=2) {
    			System.out.println(txLogs.get(i) + "\t|\t" + txLogs.get(i+1));
    		}
    	}
    	
    	/**
    	 * CS4432-Project1: add a log while an empty frame occupied
    	 */
    	public static void addMtFrmLog() {
    		mtFrmLogs.add( (Integer)mtFrmIndex );
    		mtFrmIndex ++;
    	}
    	
    	/**
    	 * CS4432-Project1: get the last used/occupied empty buffer/frame id
    	 * @return
    	 */
    	public static int getLastUsedMtFrmIndex() {
    		if(mtFrmLogs.size() > 0) {
    			return mtFrmLogs.get(mtFrmLogs.size()-1);
    		}
    		else {
    			return -1;
    		}
    	}
    	
    	/**
    	 * CS4432-Project1: print empty buffer/frame log
    	 */
    	public static void printMtFrmLogs() {
    		for(int i = 0; i<mtFrmLogs.size(); i++) {
    			System.out.println("Empty buffer: " + mtFrmLogs.get(i) + " was occupied");
    		}
    	}
    	
    	/**
    	 * CS4432-Project1: add buffer ID under LRU policy into log
    	 * @param id
    	 */
    	public static void addLRUBufferIdLog(int id) {
    		LRUBufferIdLogs.add(id);
		}
    	
    	/**
    	 * CS4432-Project1: clean up LRUBufferIdLog
    	 */
    	public static void LRUBufferIdLogCleanUp() {
    		LRUBufferIdLogs.clear();
    	}
    	
    	
    	/**
    	 * CS4432-Project1: clean up all logs
    	 */
    	public static void cleanUp() {
    		txLogs.clear();
    		mtFrmIndex = 0;
    		mtFrmLogs.clear();
    		LRUBufferIdLogs.clear();
    	}
    	
    }    
    
}
