package btree;

import chainexception.ChainException;

public class FreePageException extends ChainException {
	public FreePageException() {
		super();
	}

	public FreePageException(Exception e, String s) {
		super(e, s);
	}

	public FreePageException(String s) {
		super(null, s);
	}

}
