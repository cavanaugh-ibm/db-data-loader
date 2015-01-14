package com.cloudant.se.db.loader.write;

import com.cloudant.se.db.loader.config.DataTable;
import com.cloudant.se.db.loader.config.DataTableField;

public class FieldInstance {
	public DataTable		table;
	public DataTableField	field;
	public String			name;
	public Object			value;

	public FieldInstance(String name, Object value, DataTableField field, DataTable table) {
		this.name = name;
		this.value = value;
		this.field = field;
		this.table = table;
	}

	@Override
	public String toString() {
		return "FieldInstance [name=" + name + ", value=" + value + ", field=" + field + "]\n";
	}
}
