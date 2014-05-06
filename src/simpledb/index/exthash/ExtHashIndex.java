package simpledb.index.exthash;

import java.util.ArrayList;
import java.util.List;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

/**
 * A extensible hash implementation of the Index interface. The size of the
 * bucket can be edited by edit maxBucketSize in the field
 */
public class ExtHashIndex implements Index {
	private static int maxBucketSize = 256; // The size of the bucket
	private SecondHashIndex index; // The index for each bucket
	private Schema mySch = new Schema(); // Schema for this index, including a
											// key and a local depth
	private String myTableName = "ThisIsTestTable"; // The name of the table for
													// this index
	private boolean insertion = true; // Indicate that the current operation is
										// insertion or deletion

	private Transaction tx;
	private Constant searchkey = null;
	private TableScan ts = null;
	private Constant val;	// Current value;
	private RID rid;	// Current rid

	/**
	 * Opens a extensible hash index for the specified index and construct a new
	 * SecondHashIndex
	 * 
	 * @param idxname
	 *            the name of the index
	 * @param sch
	 *            the schema of the index records
	 * @param tx
	 *            the calling transaction
	 */
	public ExtHashIndex(String idxname, Schema sch, Transaction tx) {
		this.tx = tx;
		index = new SecondHashIndex(idxname, sch, tx);

		// Fill the field of the schema for this index
		mySch.addIntField("ExtHashIndex");
		mySch.addIntField("LocalDepth");
	}

	/**
	 * Converted input search key to the key of the bucket. Increase global
	 * depth and split and rearrange with full bucket as needed.
	 */
	public void beforeFirst(Constant searchkey) {
		// If it is the first insertion, insert the first index into record file
		if (getGlobalDepth() < 0 && insertion) {
			beforeFirstInsert();
		}

		// Convert the search key to the key of the bucket
		close();
		this.searchkey = searchkey;
		int hashCode = Integer.parseInt(searchkey.toString());
		int divisor = (int) Math.pow(2, getGlobalDepth());
		int originBucket = hashCode % divisor;
		int bucket = getSecIndex(originBucket);

		TableInfo ti = new TableInfo(myTableName, mySch);
		ts = new TableScan(ti, tx);

		// Check if the bucket if full
		boolean full = index.getNumberOfRecordsOfBucket(bucket) >= maxBucketSize;

		if (full && insertion) {
			// Get the local Depth
			int tempLocalDepth = 0;
			ts.beforeFirst();
			while (ts.next()) {
				if (ts.getInt("ExtHashIndex") == bucket) {
					tempLocalDepth = ts.getInt("LocalDepth");
				}
			}

			// Check if the global index need to be increased. If so, increase it.
			if (tempLocalDepth == getGlobalDepth()) {
				increaseGlobalDepth();
			}
			
			// Split existed record
			bucket = split(bucket);
		}

		// Execute before first in the internal index of bucket.
		index.beforeFirst(searchkey, bucket);
	}

	/**
	 * Insert the first index into record file
	 */
	private void beforeFirstInsert() {
		TableInfo ti = new TableInfo(myTableName, mySch);
		ts = new TableScan(ti, tx);
		ts.insert();
		ts.setInt("LocalDepth", 0);
		ts.setInt("ExtHashIndex", 0);
		ts.close();
	}

	/**
	 * increase the global depth and allocate new indices
	 */
	private void increaseGlobalDepth() {
		// Get the current global depth since it is dynamic generated
		int size = (int) Math.pow(2, getGlobalDepth());

		// Loop for each index to create a new index correspond to it
		for (int i = 0; i < size; i++) {
			// Get the local depth of the index
			TableInfo ti3 = new TableInfo(myTableName, mySch);
			TableScan ts3 = new TableScan(ti3, tx);
			int newkey = i + size;
			int depth = 0;
			while (ts3.next()) {
				if (ts3.getInt("ExtHashIndex") == i) {
					depth = ts3.getInt("LocalDepth");
				}
			}
			ts3.close();

			// Create the new index with same local depth
			TableInfo ti4 = new TableInfo(myTableName, mySch);
			TableScan ts4 = new TableScan(ti4, tx);
			ts4.insert();
			ts4.setInt("ExtHashIndex", newkey);
			ts4.setInt("LocalDepth", depth);
			ts4.close();
		}
	}

	/**
	 * Split one bucket to two buckets and rearrange records.
	 * 
	 * @param bucket
	 *            original bucket index
	 */
	private int split(int bucket) {
		// New key to be split into.
		int key = bucket + (int) Math.pow(2, getGlobalDepth() - 1);

		// Get the values of records in original bucket
		List<RID> tempRIDs = new ArrayList<RID>();
		List<Constant> tempKeys = new ArrayList<Constant>();
		int counter = 0;
		index.beforeFirst(bucket);
		tempRIDs = index.getAllRids();
		tempKeys = index.getAllKeys();
		index.getTs().beforeFirst();

		// Delete all records in original bucket
		while (counter < tempRIDs.size()) {
			index.getTs().beforeFirst();
			delete(tempKeys.get(counter), tempRIDs.get(counter));
			counter++;
		}

		// Get the local depth
		int depth = 0;
		ts.beforeFirst();
		while (ts.next()) {
			if (ts.getInt("ExtHashIndex") == bucket) {
				depth = ts.getInt("LocalDepth");
				ts.delete();
				break;
			}
		}

		// Delete corresponding index in record file
		ts.beforeFirst();
		while (ts.next()) {
			if (ts.getInt("ExtHashIndex") == key) {
				ts.delete();
				break;
			}
		}

		// Add corresponding index with new local depth into record file
		ts.insert();
		ts.setInt("ExtHashIndex", bucket);
		ts.setInt("LocalDepth", depth + 1);
		ts.close();

		// Add new index with local depth into record file
		TableInfo ti2 = new TableInfo(myTableName, mySch);
		TableScan ts2 = new TableScan(ti2, tx);
		ts2.insert();
		ts2.setInt("ExtHashIndex", key);
		ts2.setInt("LocalDepth", depth + 1);
		ts2.close();

		// Reinsert original values
		for (int i = 0; i < counter; i++) {
			insert(tempKeys.get(i), tempRIDs.get(i));
		}

		// Reconverts the search key and return it
		int hashCode = Integer.parseInt(searchkey.toString());
		int divisor = (int) Math.pow(2, getGlobalDepth());
		int originBucket = hashCode % divisor;
		return bucket = getSecIndex(originBucket);
	}

	/**
	 * Moves to the next record having the search key. The method loops through
	 * the table scan for the bucket, looking for a matching record, and
	 * returning false if there are no more such records.
	 * 
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
	 * Retrieves the dataRID from the current record in the table scan for the
	 * bucket.
	 * 
	 * @see simpledb.index.Index#getDataRid()
	 */
	public RID getDataRid() {
		return index.getDataRid();
	}

	/**
	 * Inserts a new record into the table scan for the bucket.
	 * 
	 * @see simpledb.index.Index#insert(simpledb.query.Constant,
	 *      simpledb.record.RID)
	 */
	public void insert(Constant val, RID rid) {
		this.val = val;
		this.rid = rid;
		insertion = true;
		
		beforeFirst(val);
		index.insert(val, rid);
		toString(val, rid);
	}

	/**
	 * Deletes the specified record from the table scan for the bucket. The
	 * method starts at the beginning of the scan, and loops through the records
	 * until the specified record is found.
	 * 
	 * @see simpledb.index.Index#delete(simpledb.query.Constant,
	 *      simpledb.record.RID)
	 */
	public void delete(Constant val, RID rid) {
		this.val = val;
		this.rid = rid;
		insertion = false;
		
		beforeFirst(val);
		index.delete(val, rid);
		toString(val, rid);
	}

	/**
	 * Closes the index by closing the current table scan.
	 * 
	 * @see simpledb.index.Index#close()
	 */
	public void close() {
		if (ts != null)
			ts.close();
	}

	/**
	 * Helper Function get second index for a tuple that will be inserted
	 * 
	 * @param extIndex
	 * @return the key of the bucket for the tuple
	 */
	public Integer getSecIndex(int extIndex) {
		TableInfo ti5 = new TableInfo(myTableName, mySch);
		TableScan ts5 = new TableScan(ti5, tx);

		// Get the local depth
		int secondHashIndex = -1;
		int localDepth = 0;
		while (ts5.next()) {
			if (ts5.getInt("ExtHashIndex") == extIndex) {
				localDepth = ts5.getInt("LocalDepth");
			}
		}
		
		// Get the key of the bucket through the local depth
		secondHashIndex = (int) (extIndex % Math.pow(2, ((double) localDepth)));
		ts5.close();
		return secondHashIndex;
	}

	/**
	 * Helper Function get current global depth dynamically
	 * 
	 * @return current global depth
	 */
	public Integer getGlobalDepth() {
		TableInfo ti = new TableInfo(myTableName, mySch);
		TableScan ts = new TableScan(ti, tx);
		int globalDepth = -1;
		
		// Count number of indices in record file
		int count = 0;
		while (ts.next()) {
			count++;
		}
		
		// Get the global depth through the number of indices
		globalDepth = (int) Math.ceil((Math.log(count) / Math.log(2)));
		ts.close();
		return globalDepth;
	}

	/**
	 * Function to print out message about the insertion or deletion
	 */
	private String toString(Constant val, RID rid) {
		// Check the action is insertion or deletion
		String action;
		if (insertion) {
			action = "insertion";
		} else
			action = "deletion";
		
		// Print messages
		System.out.println("=================================================");
		System.out.println("Do " + action + " on val=" + val + " rid=" + rid);
		System.out.println("Global Depth =" + getGlobalDepth());
		System.out.println("=================================================");
		System.out.println("");
		System.out.println("");
		
		return "Do " + action + " on val=" + val + " rid=" + rid;
	}
	
	@Override
	public String toString() {
		return toString(val, rid);
	}
}
