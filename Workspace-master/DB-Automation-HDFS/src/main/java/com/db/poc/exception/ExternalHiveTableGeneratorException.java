package com.db.poc.exception;

public class ExternalHiveTableGeneratorException extends BaseException {

	/**
	 * serialVersionUID
	 */

	private static final long serialVersionUID = -6812240116282804493L;

	/**
	 * Creates a new GalaxyShopException object.
	 */
	public ExternalHiveTableGeneratorException() {
		super();
	}

	/**
	 * Creates a new GalaxyShopException object.
	 * 
	 * @param message
	 *            Error message
	 */
	public ExternalHiveTableGeneratorException(final String message) {
		super(message);
	}

	/**
	 * Creates a new GalaxyShopException object.
	 * 
	 * @param cause
	 *            Throwable cause
	 */
	public ExternalHiveTableGeneratorException(final Throwable cause) {
		super(cause);
	}

	/**
	 * Creates a new GalaxyShopException object.
	 * 
	 * @param message
	 *            Error message
	 * @param cause
	 *            Throwable cause
	 */
	public ExternalHiveTableGeneratorException(final String message, final Throwable cause) {
		super(message, cause);
	}

	/**
	 * Creates a new GalaxyShopException object.
	 * 
	 * @param message
	 *            Error message
	 * @param cause
	 *            Throwable cause
	 * @param errorCode
	 *            Error code
	 */
	public ExternalHiveTableGeneratorException(final String message, final Throwable cause,
			final String errorCode) {
		super(message, cause, errorCode);
	}

	/**
	 * Instantiates with message and errorCode.
	 * 
	 * @param message
	 *            Error message
	 * @param errorCode
	 *            Error code
	 */
	public ExternalHiveTableGeneratorException(final String message, final String errorCode) {
		super(message, errorCode);
	}

}
