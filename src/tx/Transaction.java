package tx;

import buffer.Buffer;
import buffer.BufferMgr;
import file.BlockId;
import file.FileMgr;
import file.Page;
import log.LogMgr;
import tx.concurrency.ConcurrencyMgr;
import tx.recovery.RecoveryMgr;

public class Transaction {
	private static int nextTxNum = 0;
	private static final int END_OF_FILE = -1;
	private RecoveryMgr recoverMgr;
	private ConcurrencyMgr concurMgr;
	private BufferMgr bm;
	private FileMgr fm;
	private int txnum;
	private BufferList mybuffers;
	
	public Transaction(FileMgr fm, LogMgr lm, BufferMgr bm) {
		this.fm = fm;
		this.bm = bm;
		txnum = nextTxNumber();
		recoveryMgr = new RecoveryMgr(this, txnum, lm, bm);
		concurMgr = new ConcurrencyMgr();
		mybuffers = new BufferList(bm);
	}
	
	public void commit() {
		recoveryMgr.commit();
		System.out.println("transaction " + txnum + " committed");
		concurMgr.release();
		mybuffers.unpinAll();
	}
	
	public void rollback() {
		recoveryMgr.rollback();
		System.out.println("transaction " + txnum + " rollback");
		concurMgr.release();
		mybuffers.unpinAll();
	}
	
	public void recover() {
		bm.flushAll(txnum);
		recoveryMgr.recover();
	}
	
	public void pin(BlockId blk) {
		mybuffers.pin(blk);
	}
	
	public void unpin(BlockId blk) {
		mybuffers.unpin(blk);
	}
	
	public int getInt(BlockId blk, int offset) {
		concurMgr.sLock(blk);
		Buffer buff = mybuffers.getBuffer(blk);
		return buff.contents().getInt(offset);
	}
	
	public String getString(BlockId blk, int offset) {
		concurMgr.sLock(blk);
		Buffer buff = mybuffers.getBuffer(blk);
		return buff.contents().getString(offset);
	}

	public void setInt(BlockId blk, int offset, int val, boolean okToLog) {
		concurMgr.xLock(blk);
		Buffer buff = mybuffers.getBuffer(blk);
		int lsn = -1;
		if (okToLog)
			lsn = recoverMgr.setInt(buff, offset, val);
		Page p = buff.contents();
		p.setInt(offset, val);
		buff.setModified(txnum, lsn);
	}
	
	public void setString(BlockId blk, int offset, String val, boolean okToLog) {
		concurMgr.xLock(blk);
		Buffer buff = mybuffers.getBuffer(blk);
		int lsn = -1;
		if (okToLog)
			lsn = recoverMgr.setString(buff, offset, val);
		Page p = buff.contents();
		p.setString(offset, val);
		buff.setModified(txnum, lsn);
	}
	
	public int size(String filename) {
		BlockId dummyblk = new BlockId(filename, END_OF_FILE);
		concurMgr.sLock(dummyblk);
		return fm.length(filename);
	}
	
	public BlockId append(String filename) {
		BlockId dummyblk = new BlockId(filename, END_OF_FILE);
		concurMgr.xLock(dummyblk);
		return fm.append(filename);
	}
	
	public int blockSize() {
		return bm.bolckSize();
	}
	
	public int availableBuffs() {
		return bm.available();
	}
	
	private static synchronized int nextTxNumber() {
		nextTxNum++
		return nextTxNum;
	}
	
}
