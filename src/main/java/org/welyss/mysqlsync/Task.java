package org.welyss.mysqlsync;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Task {
	private int id;
	private String name;
	private Set<Source> sources;
	private Map<String, Destination> destinations;
	public static final String ENCODING_UTF_8 = "UTF-8";

	public Task() {
		sources = new HashSet<Source>();
		destinations = new HashMap<String, Destination>();
	}

	public void start() {
		for (Source source : sources) {
			source.start();
		}
	}

	public void stop() {
		for (Source source : sources) {
			source.stop();
		}
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

	public Set<Source> getSources() {
		return sources;
	}

	public void setSources(Set<Source> sources) {
		this.sources = sources;
	}

	public Map<String, Destination> getDestinations() {
		return destinations;
	}

	public void setDestinations(Map<String, Destination> destinations) {
		this.destinations = destinations;
	}
}
