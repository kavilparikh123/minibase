package iterator;

import chainexception.ChainException;

public class UnknownKeyTypeException extends ChainException {
	public UnknownKeyTypeException() {
		super();
	}

	public UnknownKeyTypeException(Exception e, String s) {
		super(e, s);
	}

	public UnknownKeyTypeException(String s) {
		super(null, s);
	}
}
