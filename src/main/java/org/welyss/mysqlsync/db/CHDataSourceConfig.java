package org.welyss.mysqlsync.db;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties
public class CHDataSourceConfig {
	private Map<String, Map<String, String>> datasource;

	public Map<String, Map<String, String>> getDatasource() {
		return datasource;
	}

	public void setDatasource(Map<String, Map<String, String>> datasource) {
		this.datasource = datasource;
	}
}
