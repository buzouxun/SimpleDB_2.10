package simpledb.buffer;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * 
 * @author Edward Sciore
 * 
 */
class BasicBufferMgr {
	private Buffer[] bufferpool;
	private int numAvailable;
	private LinkedList<Integer> emptyFrameIndex; // CS4432-Project1: An arraylist that
											// keeps indices of all empty frames
	private Hashtable<Integer, Integer> blockIndexTable; // CS4432-Project1: An hashtable that keeps current blocks number and its corresponding frame in the pool
	private int currentIndex;
	
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
		emptyFrameIndex = new LinkedList<Integer>();		// CS4432-Project1: XXX for YYY TODO
		blockIndexTable = new Hashtable<Integer, Integer>();	// CS4432-Project1: XXX for YYY TODO
		
		for (int i = 0; i < numbuffs; i++) {
			bufferpool[i] = new Buffer();
			emptyFrameIndex.add(i);		// CS4432-Project1: initialize the emptyFrameIndex
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
			blockIndexTable.put(buff.block().number(), currentIndex);  // CS4432-Project1: put the pair of block number and index into the hashtable
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
		blockIndexTable.put(buff.block().number(), currentIndex);  // CS4432-Project1: put the pair of block number and index into the hashtable
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
	 * CS4432-Project1: Efficient search for a given disk block by using hashtable
	 * @param blk
	 * @return
	 */
	private Buffer findExistingBuffer(Block blk) {
		Integer bufferIndex = blockIndexTable.get(blk);
		if (bufferIndex != null) {
			return bufferpool[bufferIndex];
		}
		return null;
	}

	/**
	 * CS4432-Project1: TODO
	 * @return
	 */
	private Buffer chooseUnpinnedBuffer() {
		// find an empty frame and remove it from the empty frame list
		if (!emptyFrameIndex.isEmpty()) {
			return bufferpool[emptyFrameIndex.poll()];
		}
		// find an unpinned frame
		for (int i = 0; i < bufferpool.length; i++) {
			if (!bufferpool[i].isPinned()) {
				blockIndexTable.remove(bufferpool[i].block().number());
				currentIndex = i;
				return bufferpool[i];
			}
		}
		return null;
	}
}
