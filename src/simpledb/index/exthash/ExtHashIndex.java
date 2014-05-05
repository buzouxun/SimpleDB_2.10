/**
 * add local despth, while in middle of process
 * store global depth.
 * split existed buckets
 * then check if need to increase global depth
 */
package simpledb.index.exthash;

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
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private SecondHashIndex index;
	private int globalDepth;
	private Schema mySch = null;
	private boolean insertion = true;
	
	private Constant searchkey = null;
	private TableScan ts = null;
	
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
		index = new SecondHashIndex(idxname, sch, tx);
		globalDepth = 1;
		
		mySch.addIntField("LocalDepth");
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
		close();	// TODO new close func
		this.searchkey = searchkey;
		int hashCode = searchkey.hashCode();
		int divisor = (int) Math.pow(2, globalDepth);
		int bucket = hashCode % divisor;
		
		System.out.println("bucket: " + bucket);
		
		// Check if the bucket if full
		boolean full = index.getNumberOfRecordsPerTableScan(bucket) >= 2;
		if (full && insertion) {
			split(bucket);
			// split existed record
			
			
			// check if incresase global index£¬ based on its local depth
		}
		
		String tblname = "directory" + idxname + bucket;
		TableInfo ti = new TableInfo(tblname, sch);
		ts = new TableScan(ti, tx);
		
		index.beforeFirst(searchkey, bucket);
		System.out.println("bucket = " + bucket);
		System.out.println("number = " + index.getNumberOfRecordsPerTableScan(bucket));
	}
	
	private void split(int bucket) {
		int key = bucket + (int) Math.pow(2, globalDepth - 1);
		/*String newTableName = idxname + key;
		TableInfo newTi = new TableInfo(newTableName, sch);
		TableScan newTs = new TableScan(newTi, tx);*/
		SecondHashIndex newIndex = new SecondHashIndex(idxname, sch, tx);
		
	}

	/**
	 * Moves to the next record having the search key.
	 * The method loops through the table scan for the bucket,
	 * looking for a matching record, and returning false
	 * if there are no more such records.
	 * @see simpledb.index.Index#next()
	 */
	public boolean next() {
		while (ts.next())
			if (ts.getVal("dataval").equals(searchkey))
				return true;
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
		if (ts.getVal("LocalDepth") == null) {
			ts.setInt("LocalDepth", 1);
		}
		beforeFirst(val);
		index.insert(val, rid);
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
	
	
}
