import java.util.Hashtable;

public class Tuple implements java.io.Serializable{
	Comparable[] row;
	
	public Tuple(int size) { 	
		
		//Constructor that sets the tuple's size

		row = new Comparable[size];
	}
	public Tuple(Tuple t) {
		this.row = new Comparable[t.row.length];
		for(int i = 0; i<t.row.length; i++) {
			this.row[i] = t.row[i]; 
		}
	}

    
	
	public String toString() {
		String s = "";
		for(int i = 0; i<row.length-1; i++) {
			s = s+row[i] + ",";
		}
		s = s + row[row.length-1];
		return s;
	}
}