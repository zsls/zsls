package org.pbccrc.zsls.innertrack;

import java.io.BufferedReader;
import java.util.HashSet;
import java.util.Set;

import org.pbccrc.zsls.dataload.LoadableFile;
import org.pbccrc.zsls.utils.DomainLogger;
import org.pbccrc.zsls.utils.ThreadLocalBuffer;

public class WhiteListData extends LoadableFile {
	private static DomainLogger L = DomainLogger.getLogger(WhiteListData.class.getSimpleName());

	public WhiteListData(String path) {
		super(WhiteListData.class.getSimpleName(), path);
	}
	
	private Set<String> keys;
	
	public boolean contains(String key) {
		return keys != null && keys.contains(key);
	}

	@Override
	protected void fillData(BufferedReader reader) throws Exception {
		Set<String> tmp = new HashSet<String>();
		String line = null;
		while ((line = reader.readLine()) != null) {
			if (line.isEmpty() || line.trim().length() < 5 || line.startsWith("#"))
				continue;
			tmp.add(line.trim());
		}
		this.keys = tmp;
		StringBuilder b = ThreadLocalBuffer.getLogBuilder(0);
		dumpInfo(b);
		L.info(null, b.toString());
	}
	
	public void dumpInfo(StringBuilder b) {
		b.append("whitelist members: [ ");
		for (String n : keys)
			b.append("(").append(n).append(")");
		b.append(" ]");
	}

}
