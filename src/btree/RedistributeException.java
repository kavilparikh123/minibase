package btree;

import chainexception.ChainException;

public class RedistributeException extends ChainException {
	public RedistributeException() {
		super();
	}

	public RedistributeException(Exception e, String s) {
		super(e, s);
	}

	public RedistributeException(String s) {
		super(null, s);
	}

}
