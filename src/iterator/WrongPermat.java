package iterator;

import chainexception.ChainException;

public class WrongPermat extends ChainException {
	public WrongPermat(Exception prev, String s) {
		super(prev, s);
	}

	public WrongPermat(String s) {
		super(null, s);
	}
}
