import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;


public class BinaryFile {
	
	public static void BinaryFileCreator(String name, Hashtable<String, Table> table ) throws IOException { //creates only a binary file
	    File outFile = new File (name);
	    FileOutputStream outStream = new FileOutputStream (outFile);
	    ObjectOutputStream output = new ObjectOutputStream(outStream);
	    try{
	    	output.writeObject(table);
	    }
	    catch (Exception e){
	      //System.out.println (e.getMessage());
	      e.printStackTrace();
	      System.exit(0);
	    }
	    finally{
	      outStream.close();
	    }
	    
	 }
	
	public static void BinaryFileCreator(String name,Page p ) throws IOException { //creates only a binary file
	    File outFile = new File (name);
	    FileOutputStream outStream = new FileOutputStream (outFile);
	    ObjectOutputStream output = new ObjectOutputStream(outStream);
	    try{
	    	output.writeObject(p);
	    }
	    catch (Exception e){
	      //System.out.println (e.getMessage());
	      e.printStackTrace();
	      System.exit(0);
	    }
	    finally{
	      outStream.close();
	    }
	    
	 }
	
	public static void BinaryFileCreator(String name, BPlusTree tree) throws IOException { //creates only a binary file
	    File outFile = new File (name);
	    FileOutputStream outStream = new FileOutputStream (outFile);
	    ObjectOutputStream output = new ObjectOutputStream(outStream);
	    try{
	    	output.writeObject(tree);
	    }
	    catch (Exception e){
	      //System.out.println (e.getMessage());
	      e.printStackTrace();
	      System.exit(0);
	    }
	    finally{
	      outStream.close();
	    }
	    
	 }
	public static void updateCSV(String replace1, String replace2, int row, int col1, int col2){

		try {
			File inputFile = new File("metadata.csv");
		    BufferedReader br = new BufferedReader(new FileReader(inputFile));
		    //parsing a CSV file into the constructor of Scanner class 
		    LinkedList<String[]> list = new LinkedList<>();
		    String s = br.readLine();
		    
		    	while(s!= null) {
		    		list.add(s.split(","));
		    		s = br.readLine();
		    	}
		    	br.close();
		    	list.get(row)[col1] = replace1;
		    	list.get(row)[col2] = replace2;
		    	
		    	CSVWriter writer = new CSVWriter(new FileWriter("metadata.csv"));
		    	writer.writeAll(list,false);
		    	writer.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	    	
		}
	
	 public static void writeDataAtOnce(String filePath) { //I think we need to edit this later (to get the columns names. i think we need to insert a string array to identify the title of columns)
	     // first create file object for file placed at location 
	     // specified by filepath 
		 //File file = new File(filePath+".csv");
	     try { 
	         // create FileWriter object with file as parameter 
	         FileWriter outputfile = new FileWriter(filePath+".csv", true);
	         // create CSVWriter object filewriter object as parameter 
	         CSVWriter writer = new CSVWriter(outputfile); 
	         // create a List which contains String array 
	         //List<String[]> data = new ArrayList<String[]>(); 
	         //data.add(new String[] { "Name", "Class", "Marks" }); 
	          //data.add(new String[] { "Roba", "3", "0" }); 
	         writer.writeNext(new String[] { "Name", "Class", "Marks" });
	         // closing writer connection 
	         writer.close(); 
	        } 
	     catch (IOException e){ 
	         // TODO Auto-generated catch block 
	         e.printStackTrace(); 
	       } 
	 }
	 
		public static Hashtable <String,Table> BinaryFileReaderHashtable(String name) throws IOException{ //Read data from pages(i think?)
			 File inFile = new File (name);
			 FileInputStream inStream = new FileInputStream (inFile);
			 ObjectInputStream input = new ObjectInputStream (inStream);
			 Hashtable <String,Table> p = null;
			 try{
				 while (true)
					 p = (Hashtable <String,Table>)(input.readObject());
			 	}
			 catch (EOFException e){
				 // Do nothing if it is the end of file.
			    }
			 catch (Exception e){
				 System.out.println (e.getMessage());
				 e.printStackTrace();
				 System.exit(0);
			    }			
			 finally{
				 inStream.close();	
			 }
	            return p;
		}
	 
	public static Page BinaryFileReader(String name) throws IOException{ //Read data from pages(i think?)
		 File inFile = new File (name);
		 FileInputStream inStream = new FileInputStream (inFile);
		 ObjectInputStream input = new ObjectInputStream (inStream);
		 Page p = null;
		 try{
			 while (true)
				 p = (Page)(input.readObject());
		 	}
		 catch (EOFException e){
			 // Do nothing if it is the end of file.
		    }
		 catch (Exception e){
			 System.out.println (e.getMessage());
			 e.printStackTrace();
			 System.exit(0);
		    }			
		 finally{
			 inStream.close();	
		 }
            return p;
	}
	
	public static BPlusTree BinaryFileReaderTree(String name) throws IOException{ //Read data from pages(i think?)
		 File inFile = new File (name);
		 FileInputStream inStream = new FileInputStream (inFile);
		 ObjectInputStream input = new ObjectInputStream (inStream);
		 BPlusTree t = null;
		 try{
			 while (true)
				 t = (BPlusTree)(input.readObject());
		 	}
		 catch (EOFException e){
			 // Do nothing if it is the end of file.
		    }
		 catch (Exception e){
			 System.out.println (e.getMessage());
			 e.printStackTrace();
			 System.exit(0);
		    }			
		 finally{
			 inStream.close();	
		 }
           return t;
	}
	
	public static void main (String[]args) { // for testing
		File file = new File("tables.class");
		file.delete();
	}
}
