package iterator;

import chainexception.ChainException;

public class LowMemException extends ChainException {
	public LowMemException(Exception e, String s) {
		super(e, s);
	}

	public LowMemException(String s) {
		super(null, s);
	}
}
