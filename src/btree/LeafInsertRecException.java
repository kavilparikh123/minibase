package btree;

import chainexception.ChainException;

public class LeafInsertRecException extends ChainException {
	public LeafInsertRecException() {
		super();
	}

	public LeafInsertRecException(Exception e, String s) {
		super(e, s);
	}

	public LeafInsertRecException(String s) {
		super(null, s);
	}

}
