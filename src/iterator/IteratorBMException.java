package iterator;

import chainexception.ChainException;

public class IteratorBMException extends ChainException {
	public IteratorBMException(Exception prev, String s) {
		super(prev, s);
	}

	public IteratorBMException(String s) {
		super(null, s);
	}
}
