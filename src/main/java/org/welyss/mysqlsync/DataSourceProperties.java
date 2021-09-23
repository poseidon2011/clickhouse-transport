package org.welyss.mysqlsync;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "datasource")
public class DataSourceProperties {
	private Map<String, Map<String, String>> clickhouse;

	public Map<String, Map<String, String>> getClickhouse() {
		return clickhouse;
	}

	public void setClickhouse(Map<String, Map<String, String>> clickhouse) {
		this.clickhouse = clickhouse;
	}

}
