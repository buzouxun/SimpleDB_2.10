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
 * The internal index of the extensible hash index. This index link all records in the same bucket
 * and execute insertion and deletion on the disk.
 */
public class SecondHashIndex implements Index {
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private Constant searchkey = null;
	private TableScan ts = null;
	
	/**
	 * Construct a new SecondHashIndex
	 * @param idxname the name of the index
	 * @param sch the schema of the index records
	 * @param tx the calling transaction
	 */
	public SecondHashIndex(String idxname, Schema sch, Transaction tx) {
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;
	}

	/**
	 * Override to implement the interface
	 * Note actually this index only used to help implementing extensible hash index
	 * So it would not be invoked by other methods
	 */
	public void beforeFirst(Constant searchkey) {
		
	}
	
	/**
	 * Create the TableScan with a search key before insertion and deletion
	 * @see beforeFirst() in simpledb.index.exthash.ExtHashIndex
	 */
	public void beforeFirst(Constant searchkey, int key) {
		close();
		this.searchkey = searchkey;
		String tblname = idxname + key;
		TableInfo ti = new TableInfo(tblname, sch);
		ts = new TableScan(ti, tx);	
	}
	
	/**
	 * Create the TableScan without a search key before insertion and deletion
	 * @see split() in simpledb.index.exthash.ExtHashIndex
	 */
	public void beforeFirst(int key) {
		close();
		String tblname = idxname + key;
		TableInfo ti = new TableInfo(tblname, sch);
		ts = new TableScan(ti, tx);
	}

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
		int blknum = ts.getInt("block");
		int id = ts.getInt("id");
		return new RID(blknum, id);
	}

	/**
	 * Inserts a new record into the table scan for the bucket.
	 * @see simpledb.index.Index#insert(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void insert(Constant val, RID rid) {
		ts.insert();
		ts.setInt("block", rid.blockNumber());
		ts.setInt("id", rid.id());
		ts.setVal("dataval", val);
	}

	/**
	 * Deletes the specified record from the table scan for
	 * the bucket.  The method starts at the beginning of the
	 * scan, and loops through the records until the
	 * specified record is found.
	 * @see simpledb.index.Index#delete(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void delete(Constant val, RID rid) {
		while(next())
			if (getDataRid().equals(rid)) {
				ts.delete();
				return;
			}
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
	 * @param key the key of the bucket
	 * @return the number of records in the bucket
	 */
	public int getNumberOfRecordsOfBucket(int key) {
		// Create the table scan for the bucket
		String tblname = idxname + key;
		TableInfo ti2 = new TableInfo(tblname, sch);
		TableScan ts2 = new TableScan(ti2, tx);
		
		// Count the number of records by traversing the table scan
		int n = 0;
		while (ts2.next()) {
			n++;
		}
		return n;
	}
	
	/**
	 * @return all dataval (the key of the record) of current bucket
	 */
	public List<Constant> getAllKeys() {
		List<Constant> keys= new ArrayList<Constant>();
		ts.beforeFirst();
		while(ts.next()) {
			keys.add(ts.getVal("dataval"));
		}
		return keys;
	}
	
	/**
	 * @return all RIDs of current bucket
	 */
	public List<RID> getAllRids() {
		List<RID> rids= new ArrayList<RID>();
		ts.beforeFirst();
		while(ts.next()) {
			rids.add(getDataRid());
		}
		return rids;
	}
	
	/**
	 * @return current table scan
	 */
	public TableScan getTs() {
		return ts;
	}	
}
