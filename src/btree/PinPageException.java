package btree;

import chainexception.ChainException;

public class PinPageException extends ChainException {
	public PinPageException() {
		super();
	}

	public PinPageException(Exception e, String s) {
		super(e, s);
	}

	public PinPageException(String s) {
		super(null, s);
	}

}
