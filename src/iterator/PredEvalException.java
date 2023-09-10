package iterator;

import chainexception.ChainException;

public class PredEvalException extends ChainException {
	public PredEvalException(Exception prev, String s) {
		super(prev, s);
	}

	public PredEvalException(String s) {
		super(null, s);
	}
}
