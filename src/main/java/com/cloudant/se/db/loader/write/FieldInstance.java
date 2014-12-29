package com.cloudant.se.db.loader.write;

import com.cloudant.se.db.loader.config.DataTableField;

public class FieldInstance {
	public DataTableField	field;
	public String			name;
	public Object			value;

	public FieldInstance(String name, Object value, DataTableField field) {
		this.name = name;
		this.value = value;
		this.field = field;
	}

	@Override
	public String toString() {
		return "FieldInstance [name=" + name + ", value=" + value + ", field=" + field + "]\n";
	}
}
