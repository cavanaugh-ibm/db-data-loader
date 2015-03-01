package com.cloudant.se.db.loader;

public class AppConstants {
    public enum FileType {
        CSV, JSON, XML
    }

    public enum JsonType {
        DATE, DATE_EPOCH, DATE_FORMATTED_NUMBER, DATE_FORMATTED_STRING, NUMBER, STRING
    }

    public enum NestType {
        ARRAY, OBJECT, PARENT, REFERENCE, REFERENCE_ARRAY
    }

    public enum TransformLanguage {
        GROOVY, JAVASCRIPT
    }
}
