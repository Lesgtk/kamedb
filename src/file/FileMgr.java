package file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class FileMgr {
	private File dbDirectory;
	private int blocksize;
	private boolean isNew;
	private Map<String,RandomAccessFile> openFiles = new HashMap<>();
	
	public FileMgr(File dbDirectory, int blocksize) {
		this.dbDirectory = dbDirectory;
		this.blocksize = blocksize;
		isNew = !dbDirectory.exists();
		
		// create the directory if the database is new
		if (isNew)
			dbDirectory.mkdirs();
		
		// remove any leftover temporary tables
		for (String filename :dbDirectory.list())
			if (filename.startsWith("temp"))
				new File(dbDirectory, filename).delete();
	}
	
	public synchronized void read(BlockId blk, Page p) {
		try {
			RandomAccessFile f = getFile(blk.fileName());
		}
		catch (IOException e) {
		
		}
	}
	
	private RandomAccessFile getFile(String filename) throws IOException {
		RandomAccessFile f = openFiles.get(filename);
		if (f == null) {
			File dbTable = new File(dbDirectory, filename);
			f = new RandomAccessFile(dbTable, "rws");
			openFiles.put(filename, f);
		}
		
		return f;
	}
}
