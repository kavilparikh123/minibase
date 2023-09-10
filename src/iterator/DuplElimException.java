package iterator;

import chainexception.ChainException;

public class DuplElimException extends ChainException {
	public DuplElimException(Exception prev, String s) {
		super(prev, s);
	}

	public DuplElimException(String s) {
		super(null, s);
	}
}
