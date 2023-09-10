package index;

import chainexception.ChainException;

public class UnknownIndexTypeException extends ChainException {
	public UnknownIndexTypeException() {
		super();
	}

	public UnknownIndexTypeException(Exception e, String s) {
		super(e, s);
	}

	public UnknownIndexTypeException(String s) {
		super(null, s);
	}
}
