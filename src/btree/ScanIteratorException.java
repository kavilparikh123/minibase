package btree;

import chainexception.ChainException;

public class ScanIteratorException extends ChainException {
	public ScanIteratorException() {
		super();
	}

	public ScanIteratorException(Exception e, String s) {
		super(e, s);
	}

	public ScanIteratorException(String s) {
		super(null, s);
	}

}
