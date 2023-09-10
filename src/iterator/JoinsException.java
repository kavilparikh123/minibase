package iterator;

import chainexception.ChainException;

public class JoinsException extends ChainException {
	public JoinsException(Exception prev, String s) {
		super(prev, s);
	}

	public JoinsException(String s) {
		super(null, s);
	}
}
