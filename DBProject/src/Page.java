import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Properties;
import java.util.Vector;

public class Page implements java.io.Serializable{
	Vector<Tuple> page;
	static final int size = readConfig();
	//Attributes
	
	public Page() {						//Constructor
		
		page = new Vector<Tuple>(size);
	}
	
	public static int readConfig() {
		Properties prop = new Properties();
		String fileName = "DBApp.config";
		try (FileInputStream fis = new FileInputStream(fileName)) {
		    prop.load(fis);
		} catch (FileNotFoundException ex) {
		   ex.printStackTrace(); // FileNotFoundException catch is optional and can be collapsed
		} catch (IOException ex) {
			ex.printStackTrace(); 
		    
		}
		return Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
	}
	
	public String toString() { 
		String s = "";
		for (Tuple x : page) {
			s = s+x+",";
		}
		if (s.length()!= 0)
			s = s.substring(0,s.length()-1);
		return s;
	}
	
	
	public static void main(String []args) { //main method for testing
		Vector<Integer> p=new Vector<Integer>(4);
		p.add(2);
		p.add(3);
		p.add(4);
		p.add(5);
		p.add(6);
		System.out.println(p.size());
		System.out.println(p.capacity());
		
		
	}

}