package com.cloudant.se.db.loader;

public class AppConstants {
	public enum FileType {
		CSV, JSON, XML
	}

	public enum NestType {
		ARRAY, OBJECT, PARENT, REFERENCE, REFERENCE_ARRAY
	}

	public enum TransformLanguage {
		GROOVY, JAVASCRIPT
	}
}
