
public class Range implements java.io.Serializable{
	
	Comparable min ;
	Comparable max;
	
	
	
	public Range (Comparable x ,Comparable y) {
		this.min =x ;
		this.max=y ;
	}
	
	public String toString() {
		return "("+this.min +""+","+""+this.max +")" ;
	}

}
