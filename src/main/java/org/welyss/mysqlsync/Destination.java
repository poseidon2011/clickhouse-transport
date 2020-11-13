package org.welyss.mysqlsync;

public class Destination {
	private String name;
	private Task task;
	private SQLQuene sqlQuene;

	public Destination(String name, Task task) {
		this.name = name;
		this.task = task;
	}
}
