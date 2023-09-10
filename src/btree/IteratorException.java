package btree;

import chainexception.ChainException;

public class IteratorException extends ChainException {
	public IteratorException() {
		super();
	}

	public IteratorException(Exception e, String s) {
		super(e, s);
	}

	public IteratorException(String s) {
		super(null, s);
	}

}
