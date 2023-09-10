package btree;

import chainexception.ChainException;

public class DeleteFashionException extends ChainException {
	public DeleteFashionException() {
		super();
	}

	public DeleteFashionException(Exception e, String s) {
		super(e, s);
	}

	public DeleteFashionException(String s) {
		super(null, s);
	}

}
