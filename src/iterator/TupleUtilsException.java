package iterator;

import chainexception.ChainException;

public class TupleUtilsException extends ChainException {
	public TupleUtilsException(Exception prev, String s) {
		super(prev, s);
	}

	public TupleUtilsException(String s) {
		super(null, s);
	}
}
