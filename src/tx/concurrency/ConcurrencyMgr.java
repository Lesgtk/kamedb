package tx.concurrency;

import java.util.HashMap;
import java.util.Map;

import file.BlockId;

public class ConcurrencyMgr {
	private static LockTable locktbl = new LockTable();
	private Map<BlockId, String> locks = new HashMap<BlockId, String>();
	
	public void sLock(BlockId blk) {
		if (locks.get(blk) == null)
			locktbl.sLock(blk);
		    locks.put(blk, "s");
	}
	
	public void xLock(BlockId blk) {
		if (!hasXLock(blk)) {
			sLock(blk);
			locktbl.xLock(blk);
			locks.put(blk, "x");
		}
	}
	
	public void release() {
		for (BlockId blk : locks.keySet())
			lockstbl.unlock(blk);
		locks.clear();
	}
	
	private boolean hasXLock(BlockId blk) {
		String locktype = locks.get(blk);
		return locktype != null && locktype.equals("x");
	}
}
