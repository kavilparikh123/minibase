package iterator;

import chainexception.ChainException;

public class UnknowAttrType extends ChainException {
	public UnknowAttrType(Exception prev, String s) {
		super(prev, s);
	}

	public UnknowAttrType(String s) {
		super(null, s);
	}
}
