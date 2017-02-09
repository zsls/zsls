package org.pbccrc.zsls.dataload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.pbccrc.zsls.utils.DomainLogger;

public abstract class LoadableFile implements Updatable {
	private static DomainLogger L = DomainLogger.getLogger(LoadableFile.class.getSimpleName());

	protected String name;
	protected String filePath;
	
	protected long lastModified;
	protected long fileSize;
	
	public LoadableFile(String name, String path) {
		this.name = name;
		this.filePath = path;
		this.lastModified = -1L;
		this.fileSize = -1L;
	}
	
	public String getName() {
		return name;
	}
	
	public boolean readyForUpdate() {
		File file = new File(filePath);
		if (!file.exists()) {
			L.error(null, "file not exist: " + filePath);
			return false;
		}
		long lastModified = file.lastModified();
		long fileSize = file.length();
		return (lastModified != this.lastModified || fileSize != this.fileSize);
	}
	
	public void doUpdate() {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(filePath));
		} catch (FileNotFoundException e) {
			L.error(null, "file not exists: " + filePath + ", curpath: " + System.getProperty("user.dir"));
			return;
		}
		boolean success = false;
		try {
			fillData(reader);
			success = true;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException ignore) {
			}
		}
		if (success) {
			File file = new File(filePath);
			this.lastModified = file.lastModified();
			this.fileSize = file.length();	
		}
	}
	
	public String toString() {
		return name + ", file path: " + filePath;
	}
	
	protected abstract void fillData(BufferedReader reader) throws Exception;
	
}