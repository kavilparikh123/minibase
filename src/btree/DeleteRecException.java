package btree;

import chainexception.ChainException;

public class DeleteRecException extends ChainException {
	public DeleteRecException() {
		super();
	}

	public DeleteRecException(Exception e, String s) {
		super(e, s);
	}

	public DeleteRecException(String s) {
		super(null, s);
	}

}
