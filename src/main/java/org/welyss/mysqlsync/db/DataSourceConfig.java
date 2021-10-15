package org.welyss.mysqlsync.db;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "datasource")
public class DataSourceConfig {
	private Map<String, Map<String, String>> clickhouse;

	public Map<String, Map<String, String>> getClickhouse() {
		return clickhouse;
	}

	public void setClickhouse(Map<String, Map<String, String>> clickhouse) {
		this.clickhouse = clickhouse;
	}

}
