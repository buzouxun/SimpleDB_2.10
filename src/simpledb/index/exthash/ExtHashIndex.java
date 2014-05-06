/**
 * add local despth, while in middle of process
 * store global depth.
 * split existed buckets
 * then check if need to increase global depth
 */
package simpledb.index.exthash;

import java.util.ArrayList;
import java.util.List;

import simpledb.index.Index;
import simpledb.index.hash.HashIndex;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

/**
 * CS 4432 Creation
 * 
 * A extensible hash implementation of the Index interface.
 *
 */
public class ExtHashIndex implements Index {
	private static int maxBucketSize = 128; 
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private SecondHashIndex index;
	//private int globalDepth;
	private Schema mySch = new Schema();
	private String myTableName = "ThisIsTestTable";
	private boolean insertion = true;
	
	private Constant searchkey = null;
	private TableScan ts = null;
	
//	public static String log = "my log\n";
	
	/**
	 * Opens a extensible hash index for the specified index.
	 * @param idxname the name of the index
	 * @param sch the schema of the index records
	 * @param tx the calling transaction
	 */
	public ExtHashIndex(String idxname, Schema sch, Transaction tx) {
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;
		String secondIndexName = idxname + 0;
		mySch.addIntField("ExtHashIndex");
		mySch.addIntField("LocalDepth");
		index = new SecondHashIndex(secondIndexName, sch, tx);
		//globalDepth = 1;
	}

	/**
	 * Positions the index before the first index record
	 * having the specified search key.
	 * The method hashes the search key to determine the bucket,
	 * and then opens a table scan on the file
	 * corresponding to the bucket.
	 * The table scan for the previous bucket (if any) is closed.
	 * @see simpledb.index.Index#beforeFirst(simpledb.query.Constant)
	 */
	public void beforeFirst(Constant searchkey) {
		if (getGlobalDepth() < 0) {
			beforeFirstInsert();
		}
		
		close();	// TODO new close func?
		this.searchkey = searchkey;
//		int hashCode = Integer.parseInt(searchkey.toString()) - 1;
		int hashCode = Integer.parseInt(searchkey.toString());
		System.out.println("HashCode = " + hashCode);  
		int divisor = (int) Math.pow(2, getGlobalDepth());
		System.out.println("getGlobalDepth() = " + getGlobalDepth());
		System.out.println("divisor = " + divisor);
		int originBucket = hashCode % divisor;
		int bucket = getSecIndex(originBucket);

		String tblname = "directory" + idxname + bucket;
		//TableInfo ti = new TableInfo(tblname, mySch);
		TableInfo ti = new TableInfo(myTableName, mySch);
		ts = new TableScan(ti, tx);
		
		// Check if the bucket if full
		boolean full = index.getNumberOfRecordsPerTableScan(bucket) >= maxBucketSize;
		
		if (full && insertion) {
			int tempLocalDepth = 0;
			
			System.out.println("Bucket = " + bucket);  
			// Get the local Depth
			ts.beforeFirst();
			while (ts.next()) {
				if (ts.getInt("ExtHashIndex") == bucket) {
					tempLocalDepth = ts.getInt("LocalDepth");
				}
			}
			
			ts.beforeFirst();
			while (ts.next()) {
				System.out.println("before increase = " + ts.getInt("ExtHashIndex"));
			}

			System.out.println("Before increase LocalDepth           =" + tempLocalDepth);
			System.out.println("Before increase getGlobalDepth()           =" + getGlobalDepth());
			// Check if the global index need to be increased
			if (tempLocalDepth == getGlobalDepth()) {
				increaseGlobalDepth();
			}

			System.out.println("Before Split LocalDepth           =" + tempLocalDepth);
			System.out.println("Before Split getGlobalDepth()           =" + getGlobalDepth());
			
			ts.beforeFirst();
			while (ts.next()) {
				System.out.println("after increase = " + ts.getInt("ExtHashIndex"));
			}
			// split existed record
			bucket = split(bucket);
		}
		
		index.beforeFirst(searchkey, bucket);
	}
	
	
	/**
	 * Split one bucket to two buckets and rearrange records.
	 * 
	 * @param bucket original bucket index
	 */
	private int split(int bucket) {
		int key = bucket + (int) Math.pow(2, getGlobalDepth() - 1);
		
		// Get the values of original bucket
		List<RID> tempRIDs = new ArrayList<RID>();
		List<Constant> tempKeys = new ArrayList<Constant>();
		int counter = 0;
		
		index.beforeFirst(bucket);
		tempRIDs = index.getAllRids();
		tempKeys = index.getAllKeys();
		index.getTs().beforeFirst();
		
		System.out.println(tempRIDs);
		System.out.println(tempKeys);
		
		while(counter < tempRIDs.size()) {
			index.getTs().beforeFirst();
			delete(tempKeys.get(counter), tempRIDs.get(counter));
			counter++;
		}

		ts.beforeFirst();
		while (ts.next()) {
			System.out.println("before delete = " + ts.getInt("ExtHashIndex"));
		}
		
		// Reset Local Depth
		int depth = 0;
		ts.beforeFirst();
		while (ts.next()) {
			if (ts.getInt("ExtHashIndex") == bucket) {
				depth = ts.getInt("LocalDepth");
				ts.delete();
				break;
			}
		}
		
		ts.beforeFirst();
		while (ts.next()) {
			System.out.println("in delete = " + ts.getInt("ExtHashIndex"));
		}
		
		System.out.println("bucket = " + bucket);
		System.out.println("key = " + key);
		ts.beforeFirst();
		while (ts.next()) {
			if (ts.getInt("ExtHashIndex") == key) {
				ts.delete();
				break;
			}
		}
		
		ts.beforeFirst();
		while (ts.next()) {
			System.out.println("after delete = " + ts.getInt("ExtHashIndex"));
		}

		System.out.println("Global Depth in increase1= " + getGlobalDepth());
		ts.insert();
		ts.setInt("ExtHashIndex", bucket);
		ts.setInt("LocalDepth", depth + 1);
		ts.close();
		System.out.println("Global Depth in increase2= " + getGlobalDepth());

		String tblname2 = "directory" + idxname + key;
		//TableInfo ti2 = new TableInfo(tblname2, mySch);
		TableInfo ti2 = new TableInfo(myTableName, mySch);
		TableScan ts2 = new TableScan(ti2, tx);
		ts2.insert();
		ts2.setInt("ExtHashIndex", key);
		ts2.setInt("LocalDepth", depth + 1);
		ts2.close();
		System.out.println("Global Depth in increase3= " + getGlobalDepth());

		// Reinsert original values
		for (int i = 0; i < counter; i++) {
			insert(tempKeys.get(i), tempRIDs.get(i));
		}
		System.out.println("Global Depth in increase4= " + getGlobalDepth());
		
		int hashCode = Integer.parseInt(searchkey.toString());
		System.out.println("HashCode = " + hashCode);  
		int divisor = (int) Math.pow(2, getGlobalDepth());
		System.out.println("getGlobalDepth() = " + getGlobalDepth());
		System.out.println("divisor = " + divisor);
		int originBucket = hashCode % divisor;
		return bucket = getSecIndex(originBucket);
	}
	
	/**
	 * increase the global index and allocate new indices
	 */
	private void increaseGlobalDepth() {
		int size = (int) Math.pow(2, getGlobalDepth());
		for (int i = 0; i < size; i++) {
			int depth = 0;
			String tblname3 = "directory" + idxname + i;
			//TableInfo ti3 = new TableInfo(tblname3, mySch);
			TableInfo ti3 = new TableInfo(myTableName, mySch);
			TableScan ts3 = new TableScan(ti3, tx);
			int newkey = i + size;
			while (ts3.next()) {
				if (ts3.getInt("ExtHashIndex") == i) {
					depth = ts3.getInt("LocalDepth");
				}
			}
			ts3.close();

			String tblname4 = "directory" + idxname + newkey;
			//TableInfo ti4 = new TableInfo(tblname4, mySch);
			TableInfo ti4 = new TableInfo(myTableName, mySch);
			TableScan ts4 = new TableScan(ti4, tx);
			ts4.insert();
			ts4.setInt("ExtHashIndex", newkey);
			ts4.setInt("LocalDepth", depth);
			ts4.close();
			System.out.println("End Loop " + i);
		}
		System.out.println("Global Depth in increase=" + getGlobalDepth());
	}
		
		/*SecondHashIndex newIndex = new SecondHashIndex(idxname, sch, tx);
		rearrange(newIndex, newKey);
	}
	
	private void rearrange(SecondHashIndex newIndex, int newKey) {
		List<RID> tempRID = new ArrayList<RID>();
		List<Constant> tempVal = new ArrayList<Constant>();
		int counter = 1;
		
		RID rid = index.getDataRid();
		Constant val = index.getTs().getVal("dataval");
		tempRID.add(rid);
		tempVal.add(val);
		index.delete(val, rid);
		
		while(index.next()) {
			rid = index.getDataRid();
			val = index.getTs().getVal("dataval");
			tempRID.add(rid);
			tempVal.add(val);
			index.delete(val, rid);
			counter++;
		}
		
		for (int i = 0; i < counter; i++) {
			
		}
	}*/

	/**
	 * Moves to the next record having the search key.
	 * The method loops through the table scan for the bucket,
	 * looking for a matching record, and returning false
	 * if there are no more such records.
	 * @see simpledb.index.Index#next()
	 */
	public boolean next() {
		while (ts.next()) {
			if (ts.getVal("dataval").equals(searchkey))
				return true;
		}
		return false;
	}

	/**
	 * Retrieves the dataRID from the current record
	 * in the table scan for the bucket.
	 * @see simpledb.index.Index#getDataRid()
	 */
	public RID getDataRid() {
		return index.getDataRid();
	}

	/**
	 * Inserts a new record into the table scan for the bucket.
	 * @see simpledb.index.Index#insert(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void insert(Constant val, RID rid) {
		insertion = true;
		beforeFirst(val);
		index.insert(val, rid);
		toString(val, rid);
	}
	
	private void beforeFirstInsert() {
		String tblname = "directory" + idxname + 0;
		System.out.println("CALLLLLLLLLLED");
		//TableInfo ti = new TableInfo(tblname, mySch);
		TableInfo ti = new TableInfo(myTableName, mySch);
		ts = new TableScan(ti, tx);
		ts.insert();
		ts.setInt("LocalDepth", 0);
		ts.setInt("ExtHashIndex", 0);
		ts.close();
	}

	/**
	 * Deletes the specified record from the table scan for
	 * the bucket.  The method starts at the beginning of the
	 * scan, and loops through the records until the
	 * specified record is found.
	 * @see simpledb.index.Index#delete(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void delete(Constant val, RID rid) {
		insertion = false;
		beforeFirst(val);
		index.delete(val, rid);	
		toString(val, rid);
	}

	/**
	 * Closes the index by closing the current table scan.
	 * @see simpledb.index.Index#close()
	 */
	public void close() {
		if (ts != null)
			ts.close();
	}

	/**
	 * Returns the cost of searching an index file having the
	 * specified number of blocks.
	 * The method assumes that all buckets are about the
	 * same size, and so the cost is simply the size of
	 * the bucket.
	 * @param numblocks the number of blocks of index records
	 * @param rpb the number of records per block (not used here)
	 * @return the cost of traversing the index
	 */
	public static int searchCost(int numblocks, int rpb){
		return numblocks / HashIndex.NUM_BUCKETS;
	}
	
	/**
	 * Helper Function
	 * get second index for a tuple that will be inserted
	 * TODO testing
	 * @param extIndex
	 * @return
	 */
	public Integer getSecIndex(int extIndex) {
		int secondHashIndex = -1;
		// get TableScane
		String tblname = "directory" + idxname + extIndex;
		//TableInfo ti5 = new TableInfo(tblname, mySch);
		TableInfo ti5 = new TableInfo(myTableName, mySch);
		TableScan ts5 = new TableScan(ti5, tx);
		
		/*String tblname9 = "directory" + idxname + 20;
		TableInfo ti9 = new TableInfo(tblname9, mySch);
		TableScan ts9 = new TableScan(ti9, tx);
		
		String tblname8 = "directory" + idxname + 10;
		TableInfo ti8 = new TableInfo(tblname8, mySch);
		TableScan ts8 = new TableScan(ti8, tx);*/
		// get localDepth stored in this TableScan
		System.out.println("extIndex = " + extIndex);
		int localDepth = 0;
		while(ts5.next()) {
			if (ts5.getInt("ExtHashIndex") == extIndex) {
				localDepth = ts5.getInt("LocalDepth");
			}
		}
		// compute the second hash index based on extIndax and localDepth
		secondHashIndex = (int) ( extIndex % Math.pow(2, ((double)localDepth)) );
		ts5.close();
		return secondHashIndex;
	}
	
	/**
	 * Helper Function
	 * get current global depth
	 * TODO testing
	 * @return
	 */
	public Integer getGlobalDepth() {
		int globalDepth = -1;
		// get TableScan
		String tblname = "directory" + idxname + 0;
		//TableInfo ti = new TableInfo(tblname, mySch);
		TableInfo ti = new TableInfo(myTableName, mySch);
		TableScan ts = new TableScan(ti, tx);
	
		// counting next()
		int count = 0;
		while(ts.next()) {
			count++;
			/*if (ts.getInt("ExtHashIndex") != 0 || ts.getInt("LocalDepth") != 0) {
				count++;
				System.out.println("Count = " + count);
				System.out.println("Index = " +ts.getInt("ExtHashIndex"));
				System.out.println("LocalDepth = " +ts.getInt("LocalDepth"));
			}*/
		}
		globalDepth = (int) Math.ceil((Math.log(count)/Math.log(2)));
		ts.close();
		return globalDepth;
	}
	
	private String toString(Constant val, RID rid) {
		String action;
		if (insertion) {
			action = "insertion";
		}
		else action = "deletion";
		System.out.println("Do " + action + " on val=" + val + " rid=" + rid);
		System.out.println("Global Depth =" + getGlobalDepth());
		System.out.println("");
		System.out.println("");
		System.out.println("");
		
		return "Do " + action + " on val=" + val + " rid=" + rid;
	}
}
