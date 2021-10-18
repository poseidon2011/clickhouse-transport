package org.welyss.mysqlsync.transport;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class Target {
	public static final String ENCODING_UTF_8 = "UTF-8";
	protected String name;
	protected CHHandler tMySQLHandler;
	protected Map<String, Source> sourcePool = new HashMap<String, Source>();
	public final Logger log = LoggerFactory.getLogger(getClass());
}
