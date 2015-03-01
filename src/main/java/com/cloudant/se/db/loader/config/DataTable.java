package com.cloudant.se.db.loader.config;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import com.cloudant.se.Constants;
import com.cloudant.se.db.loader.AppConstants.FileType;
import com.cloudant.se.db.loader.AppConstants.JsonType;
import com.cloudant.se.db.loader.AppConstants.NestType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.reinert.jjschema.Attributes;
import com.google.common.collect.Sets;

@Attributes(title = "DataTable", description = "A source dataset")
public class DataTable {
    @JsonIgnore
    protected static final Logger log               = Logger.getLogger(DataTable.class);

    @Attributes(required = false, description = "Should we try to cast all fields to numeric for this datasource?")
    private boolean               castNumerics      = false;

    @Attributes(required = false, description = "Set of predefined fields")
    private Set<DataTableField>   dataFields        = Sets.newLinkedHashSet();

    @Attributes(required = false, description = "Set of fields that we will use to create an _id")
    private Set<String>           dbIdFields        = Sets.newLinkedHashSet();

    @Attributes(required = false, description = "Set of fields that we will use to find our parent id")
    private Set<String>           dbParentIdFields  = Sets.newLinkedHashSet();

    @Attributes(required = false, description = "Set of files we are loading from")
    private Set<String>           fileNames         = Sets.newLinkedHashSet();

    @Attributes(required = false, description = "Type of data source")
    private FileType              fileType          = FileType.CSV;

    @Attributes(required = false, description = "Discriminator for document types")
    private String                jsonDocumentType  = null;

    @Attributes(required = false, description = "Should we store empty field:value pairs in the resultant JSON")
    private boolean               jsonIncludeEmpty  = false;

    @Attributes(required = false, description = "If we are not a parent, what field will we nest under")
    private String                jsonNestField     = null;

    @Attributes(required = true, description = "What level of nesting are we doing")
    private NestType              jsonNestType      = NestType.PARENT;

    @Attributes(required = false, description = "The field within the resulting json document that we can unique on")
    private String                jsonUniqueIdField = "_id";

    @Attributes(required = true, description = "Name of the dataset")
    private String                name              = null;

    @Attributes(required = false, description = "SQL Driver to use when pulling from this source")
    private String                sqlDriver         = null;

    @Attributes(required = false, description = "SQL Password to use when pulling from this source")
    private String                sqlPass           = null;

    @Attributes(required = false, description = "SQL Query to use when pulling from this source")
    private String                sqlQuery          = null;

    @Attributes(required = false, description = "SQL Url to use when pulling from this source")
    private String                sqlUrl            = null;

    @Attributes(required = false, description = "SQL User to use when pulling from this source")
    private String                sqlUser           = null;

    @Attributes(required = false, description = "Should we pull from a database or file?")
    private boolean               useDatabase       = false;

    public Set<DataTableField> getDataFields() {
        return dataFields;
    }

    public Set<String> getDbIdFields() {
        return dbIdFields;
    }

    public Set<String> getDbParentIdFields() {
        return dbParentIdFields;
    }

    public Set<String> getFileNames() {
        return fileNames;
    }

    public FileType getFileType() {
        return fileType;
    }

    public String getJsonDocumentType() {
        return jsonDocumentType;
    }

    public String getJsonNestField() {
        return jsonNestField;
    }

    public NestType getJsonNestType() {
        return jsonNestType;
    }

    public String getJsonUniqueIdField() {
        return jsonUniqueIdField;
    }

    public String getName() {
        return name;
    }

    public String getSqlDriver() {
        return sqlDriver;
    }

    public String getSqlPass() {
        return sqlPass;
    }

    public String getSqlQuery() {
        return sqlQuery;
    }

    public String getSqlUrl() {
        return sqlUrl;
    }

    public String getSqlUser() {
        return sqlUser;
    }

    public boolean isCastNumerics() {
        return castNumerics;
    }

    public boolean isJsonIncludeEmpty() {
        return jsonIncludeEmpty;
    }

    public boolean isUseDatabase() {
        return useDatabase;
    }

    public void setCastNumerics(boolean tryCaseNumeric) {
        this.castNumerics = tryCaseNumeric;
    }

    public void setDataFields(Set<DataTableField> dataFields) {
        this.dataFields = dataFields;
    }

    public void setDbIdFields(Set<String> idFields) {
        this.dbIdFields = idFields;
    }

    public void setDbParentIdFields(Set<String> parentIdFields) {
        this.dbParentIdFields = parentIdFields;
    }

    public void setFileNames(Set<String> fileNames) {
        this.fileNames = fileNames;
    }

    public void setFileType(FileType fileType) {
        this.fileType = fileType;
    }

    public void setJsonDocumentType(String jsonDocumentType) {
        this.jsonDocumentType = jsonDocumentType;
    }

    public void setJsonIncludeEmpty(boolean includeEmpty) {
        this.jsonIncludeEmpty = includeEmpty;
    }

    public void setJsonNestField(String nestField) {
        this.jsonNestField = nestField;
    }

    public void setJsonNestType(NestType nestType) {
        this.jsonNestType = nestType;
    }

    public void setJsonUniqueIdField(String uniqueIdField) {
        this.jsonUniqueIdField = uniqueIdField;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSqlDriver(String sqlDriver) {
        this.sqlDriver = sqlDriver;
    }

    public void setSqlPass(String sqlPass) {
        this.sqlPass = sqlPass;
    }

    public void setSqlQuery(String sqlQuery) {
        this.sqlQuery = sqlQuery;
    }

    public void setSqlUrl(String sqlUrl) {
        this.sqlUrl = sqlUrl;
    }

    public void setSqlUser(String sqlUser) {
        this.sqlUser = sqlUser;
    }

    public void setUseDatabase(boolean useDatabase) {
        this.useDatabase = useDatabase;
    }

    public void validate() {
        Assert.hasText(getName(), "Must provide a name for this table (used during logging and debugging)");

        printSetting("Name", getName());
        printSetting("Type", getJsonNestType());

        if (isUseDatabase()) {
            Assert.hasText(getSqlUrl(), "Must provide the JDBC URL of the database to pull this data from or set the default database configs");
            Assert.hasText(getSqlDriver(), "Must provide the JDBC driver of the database to pull this data from or set the default database configs");
            Assert.hasText(getSqlUser(), "Must provide the JDBC user of the database to pull this data from or set the default database configs");
            Assert.hasText(getSqlPass(), "Must provide the JDBC password of the database to pull this data from or set the default database configs");
            Assert.hasText(getSqlQuery(), "Must provide the SQL query that we will use to pull the data");
            printSetting("Source", getSqlUrl());
        } else {
            Assert.notEmpty(getFileNames(), "Must provide a data file for this table");
            Assert.notNull(getFileType(), "Must provide a type for the data files");
            printSetting("Source", getFileNames());
        }

        switch (getJsonNestType()) {
            case PARENT:
                //
                // PARENT table records must provide at the very least the idFields
                // Assert.notEmpty(idFields, "Must provide field(s) to create an _id for the records in this table");
                Assert.hasText(getJsonDocumentType(), "Must provide the type name for the created document ");
                Assert.isTrue(StringUtils.equals("_id", getJsonUniqueIdField()), "For top level documents, the idField must be \"_id\"");
                if (getDbIdFields() == null || getDbIdFields().size() == 0) {
                    setDbIdFields(new LinkedHashSet<String>());
                    getDbIdFields().add(Constants.GENERATED);
                }
                printSetting("_id fields", getDbIdFields());
                break;
            case ARRAY:
                //
                // ARRAY table records must provide at the very least the parentFields and the nestField
                Assert.notEmpty(getDbIdFields(), "Must provide field(s) to create an _id for the records in this table");
                Assert.notEmpty(getDbParentIdFields(), "Must provide field(s) to create to find the parent _id for the record that each row will be inserted into - parentIdFields");
                Assert.hasText(getJsonNestField(), "Must provide the field in the parent document that we will insert at");
                printSetting("_id fields", getDbIdFields());
                printSetting("parent _id fields", getDbParentIdFields());
                printSetting("nestField", getJsonNestField());
                break;
            case OBJECT:
                //
                // OBJECT table records must provide at the very least the parentFields and the nestField
                Assert.notEmpty(getDbParentIdFields(), "Must provide field(s) to create an _id for the record that each row will be inserted into - parentIdFields");
                Assert.hasText(getJsonNestField(), "Must provide the field in the parent document that we will insert at");
                printSetting("parent _id fields", getDbParentIdFields());
                printSetting("nestField", getJsonNestField());
                break;
            case REFERENCE:
            case REFERENCE_ARRAY:
                //
                // REFERENCE table records must provide at the very least the parentFields and the nestField and the field(s) to create an _id for the records in this table
                Assert.notEmpty(getDbParentIdFields(), "Must provide field(s) to create an _id for the record that each row will be inserted into - parentIdFields");
                Assert.hasText(getJsonNestField(), "Must provide the field in the parent document that we will insert at");
                Assert.notEmpty(getDbIdFields(), "Must provide field(s) to create an _id for the records in this table");
                Assert.hasText(getJsonDocumentType(), "Must provide the type name for the created document ");
                Assert.isTrue(StringUtils.equals("_id", getJsonUniqueIdField()), "For top level documents, the idField must be \"_id\"");
                printSetting("_id fields", getDbIdFields());
                printSetting("parent _id fields", getDbParentIdFields());
                printSetting("nestField", getJsonNestField());
                break;
            default:
                break;
        }

        setJsonUniqueIdField(StringUtils.defaultIfBlank(getJsonUniqueIdField(), "_id"));
        // System.out.println("Unique key equals - [" + idField + "][" + StringUtils.defaultIfBlank(jsonDocumentType, nestField) + "]");

        if (getDataFields() != null) {
            for (DataTableField field : getDataFields()) {
                if (castNumerics) {
                    field.setJsonType(JsonType.NUMBER);
                }

                field.validate();
            }
        }
    }

    private void printSetting(String setting, Object value) {
        log.debug("    " + setting + " --> " + value);
    }
}
