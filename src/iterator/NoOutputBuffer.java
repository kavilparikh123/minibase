package iterator;

import chainexception.ChainException;

public class NoOutputBuffer extends ChainException {
	public NoOutputBuffer(Exception prev, String s) {
		super(prev, s);
	}

	public NoOutputBuffer(String s) {
		super(null, s);
	}
}
