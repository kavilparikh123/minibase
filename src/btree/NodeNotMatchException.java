package btree;

import chainexception.ChainException;

public class NodeNotMatchException extends ChainException {
	public NodeNotMatchException() {
		super();
	}

	public NodeNotMatchException(Exception e, String s) {
		super(e, s);
	}

	public NodeNotMatchException(String s) {
		super(null, s);
	}

}
