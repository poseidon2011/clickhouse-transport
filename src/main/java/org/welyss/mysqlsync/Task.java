package org.welyss.mysqlsync;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import javax.annotation.Resource;

import java.util.Set;

import org.welyss.mysqlsync.interfaces.Parser;

public class Task {
	private int id;
	private String name;
	private Map<String, Parser> parsers;
	private Map<String, Destination> appliers;
	private Map<String, Map<String, String>> pipelines;
	public static final String ENCODING_UTF_8 = "UTF-8";

	public Task() {
		parsers = new HashMap<String, Parser>();
		appliers = new HashMap<String, Destination>();
		pipelines = new HashMap<String, Map<String, String>>();
	}

	public void start() {
		for (Entry<String, Map<String,String>> pipeline : pipelines.entrySet()) {
			String source = pipeline.getKey();
			Map<String,String> destinations = pipeline.getValue();
			Parser bp;
			if (parsers.containsKey(source)) {
				bp = parsers.get(source);
			} else {
				bp = new BinlogParser(name + "-" + source, this);
			}
			bp.start();
//			parsers.contains(o)
		}
	}

	public void stop() {
//		for (BinlogParser source : parsers) {
//			source.stop();
//		}
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
