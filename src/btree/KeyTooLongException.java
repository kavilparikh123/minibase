package btree;

import chainexception.ChainException;

public class KeyTooLongException extends ChainException {
	public KeyTooLongException() {
		super();
	}

	public KeyTooLongException(Exception e, String s) {
		super(e, s);
	}

	public KeyTooLongException(String s) {
		super(null, s);
	}

}
