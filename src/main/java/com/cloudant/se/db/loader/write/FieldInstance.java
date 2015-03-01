package com.cloudant.se.db.loader.write;

import com.cloudant.se.db.loader.config.DataTable;
import com.cloudant.se.db.loader.config.DataTableField;

public class FieldInstance {
    private DataTableField field;
    private DataTable      table;
    private Object         value;

    public FieldInstance(Object value, DataTableField field, DataTable table) {
        this.setValue(value);
        this.setField(field);
        this.setTable(table);
    }

    public DataTableField getField() {
        return field;
    }

    public DataTable getTable() {
        return table;
    }

    public Object getValue() {
        return value;
    }

    public void setField(DataTableField field) {
        this.field = field;
    }

    public void setTable(DataTable table) {
        this.table = table;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
