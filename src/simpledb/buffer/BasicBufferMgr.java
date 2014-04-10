package simpledb.buffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import simpledb.file.*;

import java.util.Hashtable;
import java.util.LinkedList;

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
	//private List<Integer> emptyFrameIndex; // CS4432-Project1: An array that
											// keeps indices of all empty frames
	private LinkedList<Integer> emptyFrameIndex;
//	private Hashtable<Integer, Integer> blockIndexTable;
	private HashMap<Integer, Integer> blockIndexTable;
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
//		emptyFrameIndex = new ArrayList<Integer>();
		numAvailable = numbuffs;
		emptyFrameIndex = new LinkedList<Integer>();
//		blockIndexTable = new Hashtable<Integer, Integer>();
		blockIndexTable = new HashMap<Integer, Integer>();
		
		for (int i = 0; i < numbuffs; i++) {
			bufferpool[i] = new Buffer(i);
			emptyFrameIndex.add(i);
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
//			blockIndexTable.put(buff.block().number(), currentIndex);// TODO new
//			blockIndexTable.put(buff.block().number(), buff.bID);// TODO new
			blockIndexTable.put(blk.hashCode(), buff.bID);// TODO new
			System.out.print("pin - blk.hashCode(): " + blk.hashCode());
//			blockIndexTable.put(blk.number(), buff.bID);// TODO new
			System.out.println("pin, buffid: " + buff.bID);	//TODO
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
//		blockIndexTable.put(buff.block().number(), currentIndex);// TODO new
//		blockIndexTable.put(buff.block().number(), buff.bID);// TODO new
		blockIndexTable.put(buff.block().hashCode(), buff.bID);// TODO new
		System.out.print("pinNew - blk.hashCode(): " + buff.block().hashCode());
//		blockIndexTable.put(buff.block().number(), buff.bID);// TODO new
		System.out.println("pinNew, buffid: " + buff.bID);	//TODO
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
		System.out.println("unpin, buffid: " + buff.bID);	//TODO
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

	private Buffer findExistingBuffer(Block blk) {
//		for (Buffer buff : bufferpool) {
//			Block b = buff.block();
//			if (b != null && b.equals(blk))
//				return buff;
//		Integer bufferIndex = blockIndexTable.get(blk);
		Integer bufferIndex = blockIndexTable.get(blk.hashCode());
//		Integer bufferIndex = blockIndexTable.get(blk.number());
		if(bufferIndex != null) {
			return bufferpool[bufferIndex];
		}
		return null;
	}

	/**
	 * CS4432-Project1: TODO
	 * @return
	 */
	private Buffer chooseUnpinnedBuffer() {
		if (!emptyFrameIndex.isEmpty()) {
//			Buffer buff = bufferpool[emptyFrameIndex.get(0)];
//			emptyFrameIndex.remove(0);
//			return buff;
			return bufferpool[emptyFrameIndex.poll()];
		}
		for(int i = 0; i < bufferpool.length; i++) {
			if(!bufferpool[i].isPinned()){
//				blockIndexTable.remove(bufferpool[i].block().number());
				blockIndexTable.remove(bufferpool[i].block().hashCode());
				System.out.print("remove - blk.hashCode(): " + bufferpool[i].block().hashCode());
//				blockIndexTable.remove(bufferpool[i].block().number());
				System.out.println("remove, buffid: " + bufferpool[i].bID);	//TODO
//				currentIndex = i;
				return bufferpool[i];
			}
		}
//		for (Buffer buff : bufferpool)
//			if (!buff.isPinned())
//				return buff;
		return null;
	}
}
