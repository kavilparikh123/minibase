package iterator;

import chainexception.ChainException;

public class SortException extends ChainException {
	public SortException(Exception e, String s) {
		super(e, s);
	}

	public SortException(String s) {
		super(null, s);
	}
}
