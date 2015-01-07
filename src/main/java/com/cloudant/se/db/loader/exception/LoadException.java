package com.cloudant.se.db.loader.exception;

public class LoadException extends RuntimeException {
	private static final long	serialVersionUID	= -1417775420488373973L;

	public LoadException(String message) {
		super(message);
	}

	public LoadException(String message, Throwable cause) {
		super(message, cause);
	}

	public LoadException(Throwable cause) {
		super(cause);
	}
}
