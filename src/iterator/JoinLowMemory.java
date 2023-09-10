package iterator;

import chainexception.ChainException;

public class JoinLowMemory extends ChainException {
	public JoinLowMemory(Exception prev, String s) {
		super(prev, s);
	}

	public JoinLowMemory(String s) {
		super(null, s);
	}
}
