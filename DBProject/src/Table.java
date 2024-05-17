import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements java.io.Serializable {
	
	String strTableName;
	String strClusteringKeyColumn;
	//Hashtable <String,String> htblColNameType;
	Hashtable <String,Range> ranges;
	
	Vector <String> pgName;
	int count; //(to name the page)
	
	public Table(String strTableName,String strClusteringKeyColumn) { //Constructor
		this.strTableName=strTableName;
		this.strClusteringKeyColumn=strClusteringKeyColumn;
		this.ranges = new Hashtable<String ,Range>();
		
		//this.htblColNameType=new Hashtable<String, String>(htblColNameType);
		
		pgName=new Vector<String>();
		
		//indexTable=new Hashtable<String,Integer>();
//		for(String s:htblColNameType.keySet()) { //to map name to index(?)
//			indexTable.put(s, i);
//			i++;	
//		}
		count=1 ; 
		
	}
	
	public void addPage (Tuple t ,int pkIndex) {
		Page p = new Page();
		p.page.add(t);
		this.ranges.put(strTableName+this.count+".class", new Range(t.row[pkIndex],t.row[pkIndex]));
		
	    try{
			File outFile = new File (strTableName+this.count+".class");
		    FileOutputStream outStream = new FileOutputStream (outFile);
			ObjectOutputStream output = new ObjectOutputStream(outStream);
	    	output.writeObject(p);
	    	outStream.close();
	    }
	    catch (Exception e){
	      System.out.println (e.getMessage());
	      e.printStackTrace();
	      System.exit(0);
	    }
	    this.pgName.add(strTableName+this.count+".class");
	    count++ ;
	    
	}
	

}