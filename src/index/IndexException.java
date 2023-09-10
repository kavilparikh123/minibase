package index;

import chainexception.ChainException;

public class IndexException extends ChainException {
	public IndexException() {
		super();
	}

	public IndexException(Exception e, String s) {
		super(e, s);
	}

	public IndexException(String s) {
		super(null, s);
	}
}
