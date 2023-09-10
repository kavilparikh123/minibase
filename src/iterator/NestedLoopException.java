package iterator;

import chainexception.ChainException;

public class NestedLoopException extends ChainException {
	public NestedLoopException(Exception prev, String s) {
		super(prev, s);
	}

	public NestedLoopException(String s) {
		super(null, s);
	}
}
