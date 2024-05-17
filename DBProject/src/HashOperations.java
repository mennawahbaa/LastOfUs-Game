import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;

public class HashOperations {
	
	public static Hashtable<String,Integer> compress(ArrayList<Hashtable<String,Integer>> values) {
		Hashtable<String,Integer> result = new Hashtable<>();
		
		for (int i = 0; i < values.size(); i++) {
		
			for (String key : values.get(i).keySet()) {
				result.put(key, values.get(i).get(key) + result.getOrDefault(key, 0));
			}
		}
		return result;
	}
	
	public static HashSet<String> intersect(HashSet<String>first,HashSet<String> second){
		if (first== null || second==null) {
			return null ;
		}
		
		HashSet<String> result= new HashSet<>() ;
		
		for(String x :first ) {
			if (second.contains(x))
					result.add(x);
			
		}
			return result ;
	}
	public static HashSet<String> union(HashSet<String>first,HashSet<String> second) {
		if (first==null) {
			return second ;
		}
		if(second == null)
			return first ;
		
		HashSet<String> result= new HashSet<>(first) ;
		for(String x :second) {
			if (!result.contains(x))
				result.add(x);
		}
		return result ;
	}
	
	public static void main(String[] args) {
		ArrayList<Hashtable<String,Integer>> al = new ArrayList<>();
		Hashtable<String,Integer> h1 = new Hashtable<>();
		Hashtable<String,Integer> h2 = new Hashtable<>();
		Hashtable<String,Integer> h3 = new Hashtable<>();
		h1.put("Page1", 2);
		h1.put("Page3", 1);
		h2.put("Page2", 4);
		h2.put("Page5", 2);
		h2.put("Page7", 2);
		h3.put("Page1", 5);
		h3.put("Page5", 3);
		al.add(h1);
		al.add(h2);
		al.add(h3);
		System.out.print(compress(al));
	}
	
}
