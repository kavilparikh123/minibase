package btree;

import chainexception.ChainException;

public class IndexSearchException extends ChainException {
	public IndexSearchException() {
		super();
	}

	public IndexSearchException(Exception e, String s) {
		super(e, s);
	}

	public IndexSearchException(String s) {
		super(null, s);
	}

}
