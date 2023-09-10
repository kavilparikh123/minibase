package btree;

import chainexception.ChainException;

public class KeyNotMatchException extends ChainException {
	public KeyNotMatchException() {
		super();
	}

	public KeyNotMatchException(Exception e, String s) {
		super(e, s);
	}

	public KeyNotMatchException(String s) {
		super(null, s);
	}

}
