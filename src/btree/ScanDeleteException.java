package btree;

import chainexception.ChainException;

public class ScanDeleteException extends ChainException {
	public ScanDeleteException() {
		super();
	}

	public ScanDeleteException(Exception e, String s) {
		super(e, s);
	}

	public ScanDeleteException(String s) {
		super(null, s);
	}

}
