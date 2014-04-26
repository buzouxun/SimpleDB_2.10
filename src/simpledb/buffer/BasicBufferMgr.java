package simpledb.buffer;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import simpledb.buffer.BufferMgr.Policy;
import simpledb.file.*;
import simpledb.server.SimpleDB;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * 
 * @author Edward Sciore
 * 
 */
class BasicBufferMgr {
	private Buffer[] bufferpool;
	private int numAvailable;
	private LinkedList<Integer> emptyFrameIndex; 			// CS4432-Project1: An arraylist that keeps indices of all empty frames
	private Hashtable<Integer, Integer> blockIndexTable; 	// CS4432-Project1: An hashtable that keeps current blocks number and its corresponding frame in the pool
	private Policy policy;									// CS4432-Project1: The buffer replacement policy

	/**
	 * CS4432-Project1: Creates a buffer manager having the specified number of
	 * buffer slots. This constructor depends on both the {@link FileMgr} and
	 * {@link simpledb.log.LogMgr LogMgr} objects that it gets from the class
	 * {@link simpledb.server.SimpleDB}. Those objects are created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or is called
	 * first.
	 * 
	 * @param numbuffs
	 *            the number of buffer slots to allocate
	 */
	BasicBufferMgr(int numbuffs) {
		bufferpool = new Buffer[numbuffs];
		numAvailable = numbuffs;
		policy = Policy.LRU;
		emptyFrameIndex = new LinkedList<Integer>();
		blockIndexTable = new Hashtable<Integer, Integer>();

		for (int i = 0; i < numbuffs; i++) {
			bufferpool[i] = new Buffer(i);
			emptyFrameIndex.add(i);				// CS4432-Project1: initialize the emptyFrameIndex
		}
	}

	/**
	 * CS4432-Project1: the BasicBufferMgr constructor with policy as input variable
	 * @param numbuffs
	 * @param policy LRU or CLOCK
	 */
	BasicBufferMgr(int numbuffs, Policy policy) {
		bufferpool = new Buffer[numbuffs];
		numAvailable = numbuffs;
		this.policy = policy;
		emptyFrameIndex = new LinkedList<Integer>();
		blockIndexTable = new Hashtable<Integer, Integer>();

		for (int i = 0; i < numbuffs; i++) {
			bufferpool[i] = new Buffer(i);
			emptyFrameIndex.add(i);				// CS4432-Project1: initialize the emptyFrameIndex
		}
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txnum
	 *            the transaction's id number
	 */
	synchronized void flushAll(int txnum) {
		for (Buffer buff : bufferpool)
			if (buff.isModifiedBy(txnum))
				buff.flush();
	}

	/**
	 * Pins a buffer to the specified block. If there is already a buffer
	 * assigned to that block then that buffer is used; otherwise, an unpinned
	 * buffer from the pool is chosen. Returns a null value if there are no
	 * available buffers.
	 * 
	 * @param blk
	 *            a reference to a disk block
	 * @return the pinned buffer
	 */
	synchronized Buffer pin(Block blk) {
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null)
				return null;
			buff.assignToBlock(blk);
			blockIndexTable.put(buff.block().hashCode(), buff.getBufferID());  // CS4432-Project1: put the pair of block number and index into the hashtable
		}
		if (!buff.isPinned()) {
			numAvailable--;
		}
		buff.pin();
		return buff;
	}

	/**
	 * Allocates a new block in the specified file, and pins a buffer to it.
	 * Returns null (without allocating the block) if there are no available
	 * buffers.
	 * 
	 * @param filename
	 *            the name of the file
	 * @param fmtr
	 *            a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		Buffer buff = chooseUnpinnedBuffer();
		if (buff == null)
			return null;
		buff.assignToNew(filename, fmtr);
		blockIndexTable.put(buff.block().hashCode(), buff.getBufferID());  // CS4432-Project1: put the pair of block number and index into the hashtable
		numAvailable--;
		buff.pin();
		return buff;
	}

	/**
	 * Unpins the specified buffer.
	 * 
	 * @param buff
	 *            the buffer to be unpinned
	 */
	synchronized void unpin(Buffer buff) {
		buff.unpin();
		if (!buff.isPinned())
			numAvailable++;
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable;
	}


	/**
	 * CS4432-Project1: Efficient search for a given disk block by using hashMap
	 * @param blk
	 * @return
	 */
	private Buffer findExistingBuffer(Block blk) {
		Integer bufferIndex = blockIndexTable.get(blk.hashCode());
		if (bufferIndex != null) {			
			return bufferpool[bufferIndex];
		}
		return null;
	}

	/**
	 * CS4432-Project1: choose unpinned buffer with priority as the following: existing buffer > empty buffer > replacing buffer
	 * @return
	 */
	private Buffer chooseUnpinnedBuffer() {
		// find an empty frame and remove it from the empty frame list
		if (!emptyFrameIndex.isEmpty()) {
			// add a log to myMetaData
			SimpleDB.myMetaData.addMtFrmLog();			
			return bufferpool[emptyFrameIndex.poll()];
		}

		// find an unpinned frame with LRU Replacement policy
		if (policy == Policy.LRU) {
			int oldestFrameTime = 2147483647;	// The time of last modified Frame
			int currentFrameTime;	// The time of the Frame on current loop
			int oldestFrameIndex = -1;	// The index of the last modified Frame

			// Find the last modified frame 
			for (int i = 0; i < bufferpool.length; i++) {
				if (!bufferpool[i].isPinned()) {
					currentFrameTime = bufferpool[i].getLogSequenceNumber();
					if (currentFrameTime < oldestFrameTime) {
						oldestFrameTime = currentFrameTime;
						oldestFrameIndex = i;
					}
				}

			}

			blockIndexTable.remove(bufferpool[oldestFrameIndex].block().hashCode());
			if (oldestFrameIndex != -1) {
				// record this buffer which is selected by LRU
				SimpleDB.myMetaData.addLRUBufferIdLog(bufferpool[oldestFrameIndex].getBufferID());
				return bufferpool[oldestFrameIndex];
			}
		}
		
		// find an unpinned frame with Clock Replacement policy
		else if (policy == Policy.CLOCK) {
			System.out.println("In clock policy");
			int k = 0;
			while (k < 2)
				for (int i = 0; i < bufferpool.length; i++) {
					if (!bufferpool[i].isPinned()) {
						if (bufferpool[i].getRefBit() == 1) {
							bufferpool[i].setRefBit(0);
						} else if (bufferpool[i].getRefBit() == 0) {
							System.out.println("Successfully return on " + k
									+ "th time");
							blockIndexTable.remove(bufferpool[i].block().hashCode());
							return bufferpool[i];
						}
					}
				}
			System.out.println(k + "th time");
			k++;
		}

		return null;
	}

	/**
	 * CS4432-Project1: get the list of buffers (id, logSequenceNumber)
	 * @return
	 */
	public List<String> get_list_buffers_ids_loqSeqNum() {
		List<String> list_buffer_ids = new ArrayList<String>();
		for(int i = 0; i < bufferpool.length; i++) {
			list_buffer_ids.add(bufferpool[i].getBufferID() + "," + bufferpool[i].getLogSequenceNumber() );
		}
		return list_buffer_ids;
	}


	/**
	 * CS4432-Project1: get the list of buffers (id, logSequenceNumber) under LRU replacement policy,
	 * which means the list is sorted upon logSequenceNumber (TimeStamp)
	 * @return
	 */
	public List<String> get_list_buffes_by_LRU() {
		List<String> list_buffer_ids = new ArrayList<String>();

		for(int i = 0; i < bufferpool.length; i++) {
			list_buffer_ids.add(bufferpool[i].getBufferID() + "," + bufferpool[i].getLogSequenceNumber() );
		}

		// sorting by logSequenceNumber
		int j = 0;
		for(int i = 1; i < list_buffer_ids.size(); i++) {
			j = i;
			while( j > 0 && Integer.parseInt(list_buffer_ids.get(j-1).split(",")[1]) > Integer.parseInt(list_buffer_ids.get(j).split(",")[1]) ) {
				String tmp_buffer_id = list_buffer_ids.get(j-1);
				list_buffer_ids.set(j -1, list_buffer_ids.get(j));
				list_buffer_ids.set(j, tmp_buffer_id);
				j = j -1;
			}
		}

		return list_buffer_ids;
	}



}
