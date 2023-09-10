package btree;

import chainexception.ChainException;

public class UnpinPageException extends ChainException {
	public UnpinPageException() {
		super();
	}

	public UnpinPageException(Exception e, String s) {
		super(e, s);
	}

	public UnpinPageException(String s) {
		super();
	}

}
