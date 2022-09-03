package tx.recovery;

import file.Page;
import log.LogMgr;
import tx.Transaction;

public class CheckpointRecord implements LogRecord {
	public CheckpointRecord() {
	}
	
	public int op() {
		return CHECKPOINT;
	}
	
	public int txNumber() {
		return -1;
	}
	
	public void undo(Transaction tx) {}
	
	public String toString() {
		return "<CHECKPOINT>";
	}
	
	public static int writeToLog(LogMgr lm) {
		byte[] rec = new byte[Integer.BYTES];
		Page p = new Page(rec);
		p.setInt(0, CHECKPOINT);
		return lm.append(rec);
	}
}
