package btree;

import chainexception.ChainException;

public class IndexFullDeleteException extends ChainException {
	public IndexFullDeleteException() {
		super();
	}

	public IndexFullDeleteException(Exception e, String s) {
		super(e, s);
	}

	public IndexFullDeleteException(String s) {
		super(null, s);
	}

}
