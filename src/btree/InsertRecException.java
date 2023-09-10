package btree;

import chainexception.ChainException;

public class InsertRecException extends ChainException {
	public InsertRecException() {
		super();
	}

	public InsertRecException(Exception e, String s) {
		super(e, s);
	}

	public InsertRecException(String s) {
		super(null, s);
	}

}
