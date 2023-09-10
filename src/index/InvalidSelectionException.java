package index;

import chainexception.ChainException;

public class InvalidSelectionException extends ChainException {
	public InvalidSelectionException() {
		super();
	}

	public InvalidSelectionException(Exception e, String s) {
		super(e, s);
	}

	public InvalidSelectionException(String s) {
		super(null, s);
	}
}
