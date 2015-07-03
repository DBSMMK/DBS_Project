package api;

public class TupleR {

	public String column;
	public Class classType;
	
	public TupleR (String col, Class cl) {
		column = col;
		classType = cl;
	}
	
	public String col() {
		return column;
	}
	
	public Class cl() {
		return classType;
	}
}
