package com.cloudant.se.db.loader.exception;

public class StructureException extends RuntimeException {
	private static final long	serialVersionUID	= -2166938256993046957L;

	public StructureException(String message) {
		super(message);
	}

	public StructureException(String message, Throwable cause) {
		super(message, cause);
	}

	public StructureException(Throwable cause) {
		super(cause);
	}
}
