package api;

public class TupleQ <T>{
	
	public int index;
	public T variable;
	
	public TupleQ (int i, T v) {
		index = i;
		variable = v;
	}

	public int i() {
		return index;
	}
	
	public T v(){
		return variable;
	}
	
}
