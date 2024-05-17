

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.Stack;
import java.util.Vector;

import com.opencsv.CSVWriter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Hashtable;
import java.lang.reflect.*;

public class DBApp {

	
	//we should add the address + location (which page has the row)
	//we want to test the b+tree to make sure of its functionality


	public DBApp( ){
	
	}
	

	// this does whatever initialisation you would like 
	// or leave it empty if there is no code you want to 
	// execute at application startup 
	public void init( ){
		
		
		Hashtable <String, Table> tables = new Hashtable <String, Table>();
		try {
			BinaryFile.BinaryFileCreator("tables.class", tables);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}


	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data 
	// type as value  
	
	
	
	
	public void createTable(String strTableName, 
							String strClusteringKeyColumn,  
		Hashtable<String,String> htblColNameType) throws DBAppException {
		try {
			Hashtable <String,Table> tables = BinaryFile.BinaryFileReaderHashtable("tables.class");
			if (tables.containsKey(strTableName)) {
				throw new DBAppException("There exist a table with the same name.");
			}
			else if (!htblColNameType.containsKey(strClusteringKeyColumn)) {
				throw new DBAppException("Invalid primary key value.");
			}
			else {
			    Table t=new Table( strTableName, strClusteringKeyColumn);
			    addToMeta(strTableName,strClusteringKeyColumn,htblColNameType);
			    tables.put(strTableName, t);
			}
			
			BinaryFile.BinaryFileCreator("tables.class", tables);
								
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		//throw new DBAppException("not implemented yet");
	}


	// following method creates a B+tree index 
	public void createIndex(String   strTableName,
							String   strColName,
							String   strIndexName) throws DBAppException{
		
		Hashtable<String, Table> tables;
		try {
			tables = BinaryFile.BinaryFileReaderHashtable("tables.class");
			if(!tables.containsKey(strTableName)) {
				throw new DBAppException("Table Does Not Exist");}
			else {
				int colIndex = 0;
				boolean tFound = false;
				boolean cFound = false;
				boolean iFound = false;
				int fileIndex = 0;
				HashSet<String> indexNames = new HashSet<>();

				try {
					BufferedReader bf = new BufferedReader(new FileReader("metadata.csv"));
					String s = bf.readLine();
					while (s != null) {
						String[] line = s.split(",");
						if (line[0].equals(strTableName)) {
							 tFound = true;
							 indexNames.add(line[4]);
							if (line[1].equals(strColName)) {
								cFound = true;
								if(line[5].equals("null")) {
									iFound = true;
								}
								break;
							}
							colIndex++;
						}	
						else if (tFound) {
							break;
						}	
						s = bf.readLine();
						fileIndex++;
					}
					bf.close();
					
				}
				 catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						
					}
					catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				
				if (!cFound) {
					throw new DBAppException("Column not found");
				}
				
				if (!iFound) {
					throw new DBAppException("There is already an index on that column");
				}
				
				if (indexNames.contains(strIndexName)) {
					throw new DBAppException("There is already an index with that name");
				}
				
				BinaryFile.updateCSV(strIndexName, "B+tree", fileIndex, 4, 5);
				
				BPlusTree tree = new BPlusTree(4); //for now
				Table table = tables.get(strTableName);
				
				for (String name : table.pgName) {
					try {
						Page p = BinaryFile.BinaryFileReader(name);
						for (Tuple tuple : p.page) {
							tree.insert(tuple.row[colIndex], name);
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				try {
					BinaryFile.BinaryFileCreator( strTableName + "_" + strIndexName + ".class", tree);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} 
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
	public void insertIntoIndices (String oldPageName, Tuple tuple, Hashtable<String, Integer> indexTable, String tableName, String pageName) {
		
		for(String indexName :indexTable.keySet()) {
			try {
				BPlusTree b = BinaryFile.BinaryFileReaderTree(tableName + "_" + indexName + ".class");
				if(oldPageName != null) {
					b.update(tuple.row[indexTable.get(indexName)], oldPageName, pageName);
				} 
				else {
					b.insert(tuple.row[indexTable.get(indexName)], pageName);
				}
				BinaryFile.BinaryFileCreator(tableName + "_" + indexName + ".class", b);
			} catch (IOException e) {
				// TODO Auto-generated catch blocks
				e.printStackTrace();
			}
		}
		
	}
	public void updateIndices (Tuple oldTuple, Tuple tuple, Hashtable<String, Integer> indexTable, String tableName, String pageName) {
		
		for(String indexName :indexTable.keySet()) {
			try {
				BPlusTree b = BinaryFile.BinaryFileReaderTree(tableName + "_" + indexName + ".class");
				b.updateKey(oldTuple.row[indexTable.get(indexName)], tuple.row[indexTable.get(indexName)], pageName);
				BinaryFile.BinaryFileCreator(tableName + "_" + indexName + ".class", b);
			} catch (IOException e) {
				// TODO Auto-generated catch blocks
				e.printStackTrace();
			}
		}
		
	}
	// following method inserts one row only. 
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, Hashtable<String,Object>  htblColNameValue) throws DBAppException {
		Hashtable<String, Table> tables;
		try {
			tables = BinaryFile.BinaryFileReaderHashtable("tables.class");
			Table t = tables.get(strTableName);
			if (t == null) {
				throw new DBAppException(strTableName + " table does not exist.");
			}
			String pk = "";
			int index = 0;
			int pkindex = 0;
			boolean found = false; 
			Queue<Pair> dtypes = new LinkedList<>();
			Hashtable<String,Integer> indexName = new Hashtable<String,Integer>();
			try {
				BufferedReader bf = new BufferedReader(new FileReader("metadata.csv"));
				String s = bf.readLine();
				while (s != null) {
					String[] line = s.split(",");
					if (line[0].equals(strTableName)) {
						found = true;
						dtypes.add(new Pair(line[1],line[2]));
						if (line[3].toLowerCase().equals("true")) {
							pk = line[1];
							pkindex = index;
							
						}
						if(!line[4].equals("null")) {
							indexName.put(line[4], index);
						}
						index++;
						
					}
					
					else if (found) {
						break;
					}	
					s = bf.readLine();
				}
				bf.close();
				//E7na HENA!!!!!!!!!!!!!!!
				if (dtypes.size() != htblColNameValue.size()) {
					throw new DBAppException("Invalid column(s)");
				}
				Tuple tuple = new Tuple(dtypes.size()); 
				
				if (!htblColNameValue.containsKey(pk))
					throw new DBAppException("No value for primary key");
						
				for (int i = 0; i < tuple.row.length; i++) {
//			
					try {
						String strColType=dtypes.peek().type;
						Class c = Class.forName(strColType);
						
						if (!htblColNameValue.containsKey(dtypes.peek().name)) {
							throw new DBAppException("Missing column");
						}
					
						if (!htblColNameValue.get(dtypes.peek().name).getClass().equals(c)) {
							throw new DBAppException("Data Types doesn't match ");
							
						}
						
						if (htblColNameValue.get(dtypes.peek().name) == null) {
							throw new DBAppException("Null values not allowed");
						}
						
						
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
				}
				tuple.row[i] = (Comparable) htblColNameValue.get(dtypes.poll().name);
				}
				if (t.pgName.isEmpty()) {
					t.addPage(tuple,pkindex);	
					insertIntoIndices (null, tuple, indexName, strTableName,t.pgName.get(0));
				}
				
				else {
					int pageIndex = -1;
					String firstPage = t.pgName.get(0);
					String lastPage = t.pgName.get(t.pgName.size()-1);
					Comparable minVal = t.ranges.get(firstPage).min;
					Comparable maxVal = t.ranges.get(lastPage).max ;
					if (((Comparable) tuple.row[pkindex]).compareTo(minVal) < 0) {
						pageIndex = 0;
						
					}
					else if (((Comparable) tuple.row[pkindex]).compareTo(maxVal) > 0) {
						pageIndex = t.pgName.size()-1 ;
							
					}
					else {
						int l = 0;
						int r = t.pgName.size() - 1;
						while (l <= r) {
							int mid = ((r-l)/2) + l;
							String p = t.pgName.get(mid);
							Comparable t1 = t.ranges.get(p).min;
							Comparable t2 = t.ranges.get(p).max;
							if (((Comparable) tuple.row[pkindex]).compareTo(t2) > 0) {
								String nextPage = t.pgName.get(mid+1);
								Comparable nextTuple = t.ranges.get(nextPage).min;
								if (((Comparable) tuple.row[pkindex]).compareTo(nextTuple) < 0) {
									pageIndex = mid;
									break;
								}
								l = mid + 1;
							}
							else if (((Comparable) tuple.row[pkindex]).compareTo(t1) < 0) {
								r = mid - 1;	
							}
							else {
								pageIndex = mid;
								break;
							}
						}	
					}
					int tupleIndex = searchPageForInsert(t, pageIndex, tuple, pkindex);
					shift(pkindex,null, t, pageIndex, tupleIndex, tuple,indexName);
					
				
				}

				tables.put(strTableName, t);
				BinaryFile.BinaryFileCreator("tables.class", tables);
				
				}
			catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	
	
	public int searchPageForInsert (Table table, int pageIndex, Tuple tuple, int pkIndex) throws DBAppException {
		Page p;
		int insertIndex = 0;
		try {
			p = BinaryFile.BinaryFileReader(table.pgName.get(pageIndex));
			int l = 0;
			int r = p.page.size() - 1;
			while (l <= r) {
				int mid =((r-l)/2) + l;
				if (((Comparable) tuple.row[pkIndex]).compareTo((Comparable)p.page.get(mid).row[pkIndex]) > 0){
					
					l = mid + 1;
					
				}
				else if (((Comparable) tuple.row[pkIndex]).compareTo((Comparable)p.page.get(mid).row[pkIndex]) < 0) {
					r = mid - 1;	
				}
				else {
					throw new DBAppException("Duplicate value for Primary Key");
				}
			}
			insertIndex = l;
		} 
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return insertIndex;
	}
	
	public int searchPageForUpdate(String pageName, Comparable clusteringKeyVal, int pkIndex ) {
		Page p;
		try {
			p = BinaryFile.BinaryFileReader(pageName);
			int l = 0;
			int r = p.page.size() - 1;
			while (l <= r) {
				int mid =((r-l)/2) + l;
				if (clusteringKeyVal.compareTo((Comparable)p.page.get(mid).row[pkIndex]) > 0){
					l = mid + 1;
				}
				
				else if (clusteringKeyVal.compareTo((Comparable)p.page.get(mid).row[pkIndex]) < 0) {
					r = mid - 1;	
				}
				else {
					return mid;
				}
			}
		} 
		
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}
	

	
	public void shift (int pkIndex,String oldPageName, Table table, int pageIndex, int tupleIndex, Tuple t, Hashtable<String, Integer> indexTable) {
		if (pageIndex == table.pgName.size()) {
			table.addPage(t,pkIndex);
			insertIntoIndices(oldPageName,t,indexTable,table.strTableName,table.pgName.get(pageIndex));
			return;
		}
		
		String pName = table.pgName.get(pageIndex);
		Page p;
		try {
			p = BinaryFile.BinaryFileReader(pName);
			if(tupleIndex == Page.size) {
				shift(pkIndex,oldPageName,table,pageIndex+1,0,t,indexTable);
				return;
			}
			
			else if(p.page.size() < Page.size) {
				if(t.row[pkIndex].compareTo(table.ranges.get(pName).min)<0) {
					Range range =table.ranges.get(pName);
					range.min=t.row[pkIndex];
					table.ranges.put(pName, range);
				}
				else if (t.row[pkIndex].compareTo(table.ranges.get(pName).max)>0) {
					Range range =table.ranges.get(pName);
					range.max=t.row[pkIndex];
					table.ranges.put(pName, range);
				}
				    
				p.page.add(tupleIndex, t);
				insertIntoIndices(oldPageName, t, indexTable,table.strTableName,pName);
				BinaryFile.BinaryFileCreator(table.pgName.get(pageIndex),p);
				return;
			}
			
			else {
				
				Tuple temp= p.page.remove(p.page.size()-1);
				if (p.page.size()==0 ) {
					Range range =table.ranges.get(pName);
					range.min=t.row[pkIndex];
					range.max=t.row[pkIndex];
					table.ranges.put(pName, range);
				}
				else {
					Range range1 =table.ranges.get(pName);
				    range1.max=p.page.get(p.page.size()-1).row[pkIndex];
					table.ranges.put(pName,range1);
				
					if(t.row[pkIndex].compareTo(table.ranges.get(pName).min)<0) {
						Range range =table.ranges.get(pName);
						range.min=t.row[pkIndex];
						table.ranges.put(pName, range);
					}
					else if (t.row[pkIndex].compareTo(table.ranges.get(pName).max)>0) {
						Range range =table.ranges.get(pName);
						range.max=t.row[pkIndex];
						table.ranges.put(pName, range);
					}
				}
				p.page.add(tupleIndex, t);
				insertIntoIndices(oldPageName,t, indexTable,table.strTableName,pName );
				BinaryFile.BinaryFileCreator(table.pgName.get(pageIndex),p);
				shift(pkIndex,pName,table,pageIndex+1,0,temp,indexTable);
			}
				
		}

		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void deleteFromIndices(String oldPageName, Tuple tuple, Hashtable<String, Integer> indexTable, String tableName) {
		for(String indexName :indexTable.keySet()) {
			try {
				BPlusTree b = BinaryFile.BinaryFileReaderTree(tableName + "_" + indexName + ".class");
				b.delete(tuple.row[indexTable.get(indexName)],oldPageName);
				BinaryFile.BinaryFileCreator(tableName + "_" + indexName + ".class", b);
			} catch (IOException e) {
				// TODO Auto-generated catch blocks
				e.printStackTrace();
			}
		}
		
	}
	
	public void updateTable(String strTableName, 
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue)  throws DBAppException{
	
		try {
			Hashtable<String, Table> tables = BinaryFile.BinaryFileReaderHashtable("tables.class");
			String pk = "";
			int index = 0;
			int pkindex = 0;
			String pkType = "";
			boolean found = false; 
			Hashtable<String, String> colDataTypes = new Hashtable<String, String>();
			Hashtable<String, Integer> colIndices = new Hashtable<String, Integer>();
			Hashtable<String,Integer> indexName = new Hashtable<String,Integer>();
			String pkIndexName="";
			BufferedReader bf = new BufferedReader(new FileReader("metadata.csv"));
			Table t = tables.get(strTableName);
			if (t == null) {
				throw new DBAppException(strTableName + " table does not exist.");
			}
			String s = bf.readLine();
			while (s != null) {
				String[] line = s.split(",");
				if (line[0].equals(strTableName)) {
					found = true;
					if (line[3].toLowerCase().equals("true")) {
						pk = line[1];
						pkindex = index; //(;-;)(T-T)
						pkIndexName = line[4];
						pkType = line[2];
					}
					if (htblColNameValue.containsKey(line[1])) {
						colDataTypes.put(line[1], line[2]);
						colIndices.put(line[1], index);
						if(!line[4].equals("null")) {
							indexName.put(line[4], index);
						}
					}
					index++;
				}
				else if (found) {
					break;
				}
				s = bf.readLine();
			}
			bf.close();
			for(String str : colDataTypes.keySet()) {
				try {
					String strColType=colDataTypes.get(str);
					Class c = Class.forName(strColType);
					if(!htblColNameValue.get(str).getClass().equals(c)) {
						throw new DBAppException("Data Types doesn't match ");
						
					}
					
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			}
			}
			Comparable valueOfPK = strClusteringKeyValue;
			try {
				if(pkType.equals("java.lang.Integer")){
					valueOfPK = Integer.parseInt(strClusteringKeyValue);
				}
				else if(pkType.equals("java.lang.Double")) {
					valueOfPK = Double.parseDouble(strClusteringKeyValue);
				}
			}catch(Exception e ) {
				throw new DBAppException("Invalid format for primary key!");
			}
			String pageName = "";
			Page p = null;
			if(!pkIndexName.equals("null")) {
				BPlusTree bTree = BinaryFile.BinaryFileReaderTree(strTableName+"_"+pkIndexName+".class");
		
				if(bTree.search(valueOfPK) == null) {
					throw new DBAppException("Primary key not found!");
				}
				else {
					for(String str: bTree.search(valueOfPK).keySet()) {
						pageName = str;
					}
					
				}
			}
			else {
				int l = 0;
				int r = t.pgName.size() - 1;
				int pageIndex = -1;
				while (l <= r) {
					int mid = ((r-l)/2) + l;
					String page = t.pgName.get(mid);
					Comparable t1 = t.ranges.get(page).min;
					Comparable t2 = t.ranges.get(page).max;
					if (valueOfPK.compareTo(t2) > 0) {
						l = mid + 1;
					}
					else if (valueOfPK.compareTo(t1) < 0) {
						r = mid - 1;	
					}
					else {
						pageIndex = mid;
						break;
					}
				}
				if(pageIndex == -1) {
					throw new DBAppException("No page was found!");
				}
				else {
					pageName = t.pgName.get(pageIndex);
				}
				
			}
			Tuple tuple = null;
			int tupleIndex =searchPageForUpdate(pageName,valueOfPK,pkindex);
			if(tupleIndex == -1) {
				throw new DBAppException("Primary key value does not exist!");
			}
			else {
				p = BinaryFile.BinaryFileReader(pageName);
				tuple = p.page.get(tupleIndex);
				Tuple oldTuple = new Tuple(tuple);
				for(String str : htblColNameValue.keySet()) {
					tuple.row[colIndices.get(str)] = (Comparable) htblColNameValue.get(str);
				}
				p.page.set(tupleIndex, tuple);
				BinaryFile.BinaryFileCreator(pageName, p);	
				updateIndices(oldTuple, tuple, indexName, strTableName, pageName);
			}
		
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName,Hashtable<String,Object> htblColNameValue) throws DBAppException{
		Hashtable<String, Table> tables;
		String pk = "";
		int index = 0;
		int pkindex = 0;
		String pkIndexName="";
		boolean found = false; 
		Hashtable<String, String> colDataTypes = new Hashtable<String, String>();
		Hashtable<String, Integer> colIndices = new Hashtable<String, Integer>();
		Hashtable<String,Integer> indexName = new Hashtable<String,Integer>();
		Hashtable<String,String> colToIndexMapping = new Hashtable<String,String>();
		try {
			tables = BinaryFile.BinaryFileReaderHashtable("tables.class");
			Table t = tables.get(strTableName);
			if (t == null) {
				throw new DBAppException(strTableName + " table does not exist.");
				}
			
			BufferedReader bf = new BufferedReader(new FileReader("metadata.csv"));
			String s = bf.readLine();
			while (s != null) {
				String[] line = s.split(",");
				if (line[0].equals(strTableName)) {
					found = true;
					if (htblColNameValue.containsKey(line[1])) {
						colDataTypes.put(line[1], line[2]);
						colIndices.put(line[1], index);
						if(!line[4].equals("null")) {
							colToIndexMapping.put(line[1], line[4]);
						}
					}
					if(!line[4].equals("null")) {
						indexName.put(line[4], index);
					}
					if (line[3].toLowerCase().equals("true")) {
						pk = line[1];
						pkindex = index;
						pkIndexName= line[4];
						
					}
					if(!line[4].equals("null")) {
						indexName.put(line[4], index);
					}
					index++;	
				}
				
				else if (found) {
					break;
				}	
				s = bf.readLine();
			}
			bf.close();
			for(String str : colDataTypes.keySet()) {
				try {
					String strColType=colDataTypes.get(str);
					Class c = Class.forName(strColType);
					if(!htblColNameValue.get(str).getClass().equals(c)) {
						throw new DBAppException("Data Types doesn't match ");
						
					}
					
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (htblColNameValue.isEmpty()) {
				
				for (int i = 0; i < t.pgName.size(); i++) {
					File file = new File(t.pgName.get(i));
					file.delete();
				}
				for (String iName : indexName.keySet()) {
					BPlusTree bt = new BPlusTree(4);
					BinaryFile.BinaryFileCreator(strTableName + "_" + iName + ".class", bt);
				}
				t.pgName.clear();
				t.ranges.clear();
				
			}
			
			else if (htblColNameValue.containsKey(pk)) {
				String pageName = null;
				Comparable valueOfPK = (Comparable) htblColNameValue.get(pk);
				if(!pkIndexName.equals("null")) {
					BPlusTree bTree = BinaryFile.BinaryFileReaderTree(strTableName+"_"+pkIndexName+".class");
					for(String str : bTree.search(valueOfPK).keySet()) {
						pageName = str;
					}
					if(pageName == null) {
						return;
					}
				}
				else {
					int l = 0;
					int r = t.pgName.size() - 1;
					int pageIndex = -1;
					while (l <= r) {
						int mid = ((r-l)/2) + l;
						String page = t.pgName.get(mid);
						Comparable t1 = t.ranges.get(page).min;
						Comparable t2 = t.ranges.get(page).max;
						if (valueOfPK.compareTo(t2) > 0) {
							l = mid + 1;
						}
						else if (valueOfPK.compareTo(t1) < 0) {
							r = mid - 1;	
						}
						else {
							pageIndex = mid;
							break;
						}
					}
					if(pageIndex == -1) {
						return;
					}
					else {
						pageName = t.pgName.get(pageIndex);
					}	
				}
				int tupleIndex = searchPageForUpdate(pageName,valueOfPK, pkindex);
				if(tupleIndex == -1)
					return;
				
				Page p = BinaryFile.BinaryFileReader(pageName);
				Tuple tuple = p.page.get(tupleIndex);
				if(deleteTupleFromPage(pkindex,p, tuple, htblColNameValue, indexName, colIndices, strTableName, tupleIndex, t, pageName) != 1) {
					BinaryFile.BinaryFileCreator(pageName, p);	
				}
				
			}		
			else {
				if(colToIndexMapping.isEmpty()) {
					for(int i=0; i<t.pgName.size(); i++) {
						String pageName =t.pgName.get(i);
						Page temp = BinaryFile.BinaryFileReader(pageName);
						for(int j = 0; j<temp.page.size(); j++) {
							Tuple tuple = temp.page.get(j);
							int res = deleteTupleFromPage(pkindex,temp, tuple, htblColNameValue, indexName, colIndices, strTableName, j,t, pageName);
							if (res == 1)
								i--;
							else if (res == 2) {
								j--;
							}
						}
						if(!temp.page.isEmpty()) {
							BinaryFile.BinaryFileCreator(pageName, temp);	
						}
					}
						
				}
				else {
				    boolean flag = false; 
					Hashtable<String,BPlusTree> allIndices = new Hashtable<>();
					BPlusTree b = null;
					String bIndex = "";
					HashSet<String> pages = null;
					for(String str: colToIndexMapping.keySet()) {
						BPlusTree bTree = BinaryFile.BinaryFileReaderTree(strTableName+"_"+colToIndexMapping.get(str)+".class");
						if(!flag) {
							b = bTree;
							bIndex = str;
							flag = true;
						}
						else {
							allIndices.put(str, bTree);
						}
					}
					
					
					pages =new HashSet<>(b.search((Comparable) htblColNameValue.get(bIndex)).keySet());
					for(String x : allIndices.keySet()) {
						BPlusTree tree= allIndices.get(x);
						HashSet<String> searRes = new HashSet<>(tree.search((Comparable) htblColNameValue.get(x)).keySet());
						pages = HashOperations.intersect(pages, searRes);
					}
					for(String pageName : pages) {
						Page pg = BinaryFile.BinaryFileReader(pageName);
						for(int i = 0; i< pg.page.size(); i++) {
							int res = deleteTupleFromPage(pkindex,pg, pg.page.get(i), htblColNameValue, indexName, colIndices, strTableName, i, t, pageName);
							if (res == 2) {
								i--;
							}
							
						}
						if(!pg.page.isEmpty()) {
							BinaryFile.BinaryFileCreator(pageName, pg);	
						}
					
					
					}
					
				}	
			
					
				
			}
			tables.put(strTableName, t);
			BinaryFile.BinaryFileCreator("tables.class", tables);
		  
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}

	public int deleteTupleFromPage (int pkIndex ,Page p,Tuple tuple, Hashtable<String, Object> htblColNameValue,
		Hashtable<String, Integer> indexName,Hashtable<String, Integer> colIndices,String strTableName,
		int tupleIndex,Table t, String pageName ) {
		for(String str : htblColNameValue.keySet()) {
			if(tuple.row[colIndices.get(str)].compareTo((Comparable) htblColNameValue.get(str))!=0)
				return 0;
		}
		deleteFromIndices(pageName, tuple, indexName, strTableName);

		
		p.page.remove(tupleIndex);
	
	
		if(p.page.isEmpty()) {
			t.pgName.remove(pageName);
			t.ranges.remove(pageName);
			File file = new File(pageName);
			file.delete();
			return 1;
		}
		if (tuple.row[pkIndex].compareTo(t.ranges.get(pageName).min)==0) {
			  
			  Range g=t.ranges.get(pageName); 
			  g.min=p.page.get(0).row[pkIndex];
			  t.ranges.put(pageName, g);}
		else if (tuple.row[pkIndex].compareTo(t.ranges.get(pageName).max)==0) {
			 Range g=t.ranges.get(pageName); 
			  g.max=p.page.get(tupleIndex-1).row[pkIndex];
			  t.ranges.put(pageName, g);
		}
		return 2;
	}
		
	

	
	
	
	
	
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException{
		if(arrSQLTerms == null || arrSQLTerms.length == 0) {
			throw new DBAppException("Expression cannot be evaluated!");
		}
		if((strarrOperators == null || strarrOperators.length == 0 )&& arrSQLTerms.length != 1) {
			throw new DBAppException("Expression cannot be evaluated!");
		}
		if(strarrOperators.length != arrSQLTerms.length -1) {
			throw new DBAppException("Expression cannot be evaluated!");
		}
		HashSet<String> htblColNameValue = new HashSet<String>();
		String firstTableName = arrSQLTerms[0]._strTableName;
		for(int i = 0; i<arrSQLTerms.length;i++ ) {
			if(!arrSQLTerms[i]._strTableName.equals(firstTableName))
			    throw new DBAppException("Expression cannot be evaluated. Tables are not the same!");
			htblColNameValue.add(arrSQLTerms[i]._strColumnName);
		}
		try {
			Hashtable<String, Table> tables = BinaryFile.BinaryFileReaderHashtable("tables.class");
			String pk = "";
			int index = 0;
			int pkindex = 0;
			String pkType = "";
			boolean found = false; 
			Hashtable<String, String> colDataTypes = new Hashtable<String, String>();
			Hashtable<String, Integer> colIndices = new Hashtable<String, Integer>();
			Hashtable<String, String> indexName = new Hashtable<String, String>();
			String pkIndexName = "";
			BufferedReader bf = new BufferedReader(new FileReader("metadata.csv"));
			Table t = tables.get(firstTableName);
			if (t == null) {
				throw new DBAppException(firstTableName + " table does not exist.");
			}
			String s = bf.readLine();
			while (s != null) {
				String[] line = s.split(",");
				if (line[0].equals(firstTableName)) {
					found = true;
					if (line[3].toLowerCase().equals("true")) {
						pk = line[1];
						pkindex = index; //(;-;)(T-T)
						pkIndexName = line[4];
						pkType = line[2];
					}
					if (htblColNameValue.contains(line[1])) {
						colDataTypes.put(line[1], line[2]);
						colIndices.put(line[1], index);
						if(!line[4].equals("null")) {
							indexName.put(line[1], line[4]);
						}
					}
					index++;
				}
				else if (found) {
					break;
				}
				s = bf.readLine();
			}
			bf.close();
			for(int i = 0; i < arrSQLTerms.length; i++) {
				try {
					String strColType=colDataTypes.get(arrSQLTerms[i]._strColumnName);
					Class c = Class.forName(strColType);
					if (!arrSQLTerms[i]._objValue.getClass().equals(c)) {
						throw new DBAppException("Data Types don't match!");	
					}
					String op = arrSQLTerms[i]._strOperator;
					if (!(op.equals("=") || op.equals(">=") || op.equals("<=") || op.equals(">") || op.equals("<") || op.equals("!="))) {
						throw new DBAppException("Invalid Operator!");
					}
				}catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				}
			}
			//(:~:)
			Vector<Tuple>resultSet=new Vector<>() ;
			Hashtable<String, BPlusTree> indices= new Hashtable<String, BPlusTree>();
			HashSet <String> finalPages =evaluateSets(resPages( arrSQLTerms,  indexName,  pk,  pkindex, indices),strarrOperators);
			for(String x :finalPages) {
				Page p =BinaryFile.BinaryFileReader(x);
				for(Tuple tuple : p.page) {
					if(evaluateBooleanExpression(evaluateWhereClause( tuple,  arrSQLTerms,  colIndices) , strarrOperators )) {
						resultSet.add(tuple);
					}
				}
				
			}
			return  resultSet.iterator() ;
			
		
		
		}
		
		
		catch (IOException e) {
				// TODO Auto-generated catch block
			
			
				e.printStackTrace();
		}
		
		
	   return null; 
	}
	
	public HashSet<String>[] resPages(SQLTerm[] terms, Hashtable<String, String> indexName, String pk, int pkindex, Hashtable<String, BPlusTree> indices){
		HashSet<String>[]result=new HashSet[terms.length];
		for(int i=0 ;i<terms.length;i++) {
			result[i]=pagesToSearchIn(terms[i], indexName, pk, pkindex, indices);
		}
		return result ;
	}

	public HashSet<String> evaluateSets(HashSet<String>[]operands, String[] operators ) throws DBAppException{
		Stack<HashSet<String>> operandStack = new Stack<>();
		Stack<String> operatorStack = new Stack<>();
		operandStack.push(operands[0]);
		for (int i = 0; i < operands.length - 1; i++) {
			while (!operatorStack.isEmpty() && priority(operators[i]) <= priority(operatorStack.peek())) {
				HashSet<String> second = operandStack.pop();
				HashSet<String> first = operandStack.pop();
				String operator = operatorStack.pop();
				operandStack.push(performSetOperation(first, second, operator));
			}
			if (!(operators[i].toLowerCase().equals("xor") || operators[i].toLowerCase().equals("or") || operators[i].toLowerCase().equals("and"))) {
				throw new DBAppException("Invalid operator");
			}
			operatorStack.push(operators[i]);
			operandStack.push(operands[i+1]);
		}
		while (!operatorStack.isEmpty()) {
			HashSet<String>second = operandStack.pop();
			HashSet<String> first = operandStack.pop();
			String operator = operatorStack.pop();
			operandStack.push(performSetOperation(first, second, operator));
		}
		return operandStack.pop();
	
	}
	
	public HashSet<String> performSetOperation(HashSet<String> first ,HashSet<String> second ,String operator){
		if (operator.toLowerCase().equals("and"))
			return HashOperations.intersect(first, second);
		else {
			return HashOperations.union(first, second);
		}
	}
	
	public HashSet<String> pagesToSearchIn(SQLTerm term, Hashtable<String, String> indexName, String pk, int pkindex, Hashtable<String, BPlusTree> indices) {
		if (term._strOperator.equals("!=") || !(term._strColumnName.equals(pk) || indexName.containsKey(term._strColumnName))) {
			try {
				Table table = BinaryFile.BinaryFileReaderHashtable("tables.class").get(term._strTableName);
				HashSet<String> pages = new HashSet<>(table.pgName);
				return pages;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if (indexName.containsKey(term._strColumnName)){
			BPlusTree bt = null;
			if (!indices.containsKey(term._strColumnName)) {
				try {
					String tableName=term._strTableName;
					bt = BinaryFile.BinaryFileReaderTree(tableName+"_"+indexName.get(term._strColumnName)+".class");
					indices.put(term._strColumnName, bt);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else {
				bt = indices.get(term._strColumnName);
			}
			
			if (term._strOperator.equals("=")) {
				Hashtable<String,Integer> searchres=bt.search((Comparable) term._objValue);
				
				if(searchres!=null) {
					return new HashSet<>(bt.search((Comparable) term._objValue).keySet());
					
				 }
				return null ;
			}
			else if (term._strOperator.equals(">=")) {
				Hashtable<String, Integer> ht = HashOperations.compress(bt.search((Comparable) term._objValue, bt.maximumKey()));
				return new HashSet<>(ht.keySet());
			}
			else if (term._strOperator.equals("<=")) {
				Hashtable<String, Integer> ht = HashOperations.compress(bt.search(bt.minimumKey(), (Comparable) term._objValue));
				return new HashSet<>( ht.keySet());
			}
			else if (term._strOperator.equals(">")) {
				Hashtable<String, Integer> ht = HashOperations.compress(bt.searchStrictlyGreater(bt.minimumKey(), (Comparable) term._objValue));
				 return new HashSet<>( ht.keySet());
			}
			else {
				Hashtable<String, Integer> ht = HashOperations.compress(bt.searchStrictlyLess(bt.minimumKey(), (Comparable) term._objValue));
				return new HashSet<>( ht.keySet());
			}
		}
		else {
			try {
				Table t = BinaryFile.BinaryFileReaderHashtable("tables.class").get(term._strTableName);
				int l = 0;
				int r = t.pgName.size() - 1;
				int pageIndex = -1;
				if (term._strOperator.equals("=")) {
					while (l <= r) {
						int mid = ((r-l)/2) + l;
						String page =t.pgName.get(mid);
						Comparable t1 = t.ranges.get(page).min;
						Comparable t2 = t.ranges.get(page).max;
						if (((Comparable) term._objValue).compareTo(t2) > 0) {
							l = mid + 1;
						}
						else if (((Comparable) term._objValue).compareTo(t1) < 0) {
							r = mid - 1;	
						}
						else {
							pageIndex = mid;
							break;
						}
					}
					if (pageIndex == -1) {
						return null;
					}
					else {
						HashSet<String> res = new HashSet<>();
						res.add(t.pgName.get(pageIndex));
						return res; 
					}
				}
				else if (term._strOperator.equals(">=")) {
					while (l <= r) {
						int mid = ((r-l)/2) + l;
						String page = t.pgName.get(mid);
						Comparable t1 = t.ranges.get(page).min;
						Comparable t2 = t.ranges.get(page).max;
						if (((Comparable) term._objValue).compareTo(t2) > 0) {
							l = mid + 1;
						}
						else if (((Comparable) term._objValue).compareTo(t1) < 0) {
							r = mid - 1;
							pageIndex = mid;
						}
						else {
							pageIndex = mid;
							break;
						}
					}
					if (pageIndex == -1) {
						return null;
					}
					HashSet<String> pages = new HashSet<>(t.pgName.subList(pageIndex, t.pgName.size()));
					return pages;	
				}
				else if (term._strOperator.equals("<=")) {
					while (l <= r) {
						int mid = ((r-l)/2) + l;
						String page =t.pgName.get(mid);
						Comparable t1 = t.ranges.get(page).min;
						Comparable t2 = t.ranges.get(page).max;
						if (((Comparable) term._objValue).compareTo(t2) > 0) {
							l = mid + 1;
							pageIndex = mid;
						}
						else if (((Comparable) term._objValue).compareTo(t1) < 0) {
							r = mid - 1;
						}
						else {
							pageIndex = mid;
							break;
						}
					}
					if (pageIndex == -1) {
						return null;
					}
					HashSet<String> pages = new HashSet<>(t.pgName.subList(0, pageIndex + 1));
					return pages;	
				}
				else if (term._strOperator.equals(">")) {
					while (l <= r) {
						int mid = ((r-l)/2) + l;
						String page = t.pgName.get(mid);
						Comparable t1 = t.ranges.get(page).min;
						Comparable t2 = t.ranges.get(page).max;
						if (((Comparable) term._objValue).compareTo(t2) > 0) {
							l = mid + 1;
						}
						else if (((Comparable) term._objValue).compareTo(t1) < 0) {
							r = mid - 1;
							pageIndex = mid;
						}
						else {
							if (((Comparable) term._objValue).compareTo(t2) == 0) {
								pageIndex = mid+1;
								break;
							}
							pageIndex = mid;
							break;
						}
					}
					if (pageIndex == -1) {
						return null;
					}
					HashSet<String> pages = new HashSet<>(t.pgName.subList(pageIndex, t.pgName.size()));
					return pages;
				}
				else {
					while (l <= r) {
						int mid = ((r-l)/2) + l;
						String page = t.pgName.get(mid);
						Comparable t1 = t.ranges.get(page).min;
						Comparable t2 = t.ranges.get(page).max;
						if (((Comparable) term._objValue).compareTo(t2) > 0) {
							l = mid + 1;
						}
						else if (((Comparable) term._objValue).compareTo(t1) < 0) {
							r = mid - 1;
							pageIndex = mid;
						}
						else {
							if (((Comparable) term._objValue).compareTo(t1) == 0) {
								pageIndex = mid-1;
								break;
							}
							pageIndex = mid;
							break;
						}
					}
					if (pageIndex == -1) {
						return null;
					}
					HashSet<String> pages = new HashSet<>(t.pgName.subList(0, pageIndex+1));
					return pages;
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}
	
	public static boolean evaluateCondition(Tuple t, SQLTerm term, Hashtable<String, Integer> colIndices) {
		String col = term._strColumnName;
		String operator = term._strOperator;
		Comparable value = (Comparable) term._objValue;
		int index = colIndices.get(col);
		if (operator.equals("=")) {
			return t.row[index].compareTo(value) == 0;
		}
		else if (operator.equals("!=")){
			return t.row[index].compareTo(value) != 0;
		}
		else if (operator.equals(">=")) {
			return t.row[index].compareTo(value) >= 0;
		}
		else if (operator.equals("<=")) {
			return t.row[index].compareTo(value) <= 0;
		}
		else if (operator.equals(">")) {
			return t.row[index].compareTo(value) > 0;
		}
		else {
			return t.row[index].compareTo(value) < 0;
		}
	}
	
	public static boolean[] evaluateWhereClause(Tuple t, SQLTerm[] terms, Hashtable<String, Integer> colIndices) {
		boolean[] result = new boolean[terms.length];
		for (int i = 0; i < terms.length; i++) {
			result[i] = evaluateCondition(t, terms[i], colIndices);
		}
		return result;
	}
	
 	public static boolean evaluateBooleanExpression(boolean[] operands, String[] operators) throws DBAppException {
		Stack<Boolean> operandStack = new Stack<>();
		Stack<String> operatorStack = new Stack<>();
		operandStack.push(operands[0]);
		for (int i = 0; i < operands.length - 1; i++) {
			while (!operatorStack.isEmpty() && priority(operators[i]) <= priority(operatorStack.peek())) {
				boolean second = operandStack.pop();
				boolean first = operandStack.pop();
				String operator = operatorStack.pop();
				operandStack.push(performOperation(first, second, operator));
			}
			if (!(operators[i].toLowerCase().equals("xor") || operators[i].toLowerCase().equals("or") || operators[i].toLowerCase().equals("and"))) {
				throw new DBAppException("Invalid operator");
			}
			operatorStack.push(operators[i]);
			operandStack.push(operands[i+1]);
		}
		while (!operatorStack.isEmpty()) {
			boolean second = operandStack.pop();
			boolean first = operandStack.pop();
			String operator = operatorStack.pop();
			operandStack.push(performOperation(first, second, operator));
		}
		return operandStack.pop();
	}
	
	public static int priority(String operator) throws DBAppException {
		if (operator.toLowerCase().equals("xor")) {
			return 1;
		}
		else if (operator.toLowerCase().equals("or")) {
			return 2;
		}
		else {
			return 3;
		}
	}
	
	public static boolean performOperation(boolean first, boolean second, String operator) {
		if (operator.toLowerCase().equals("and")) {
			return first && second;
		}
		else if (operator.toLowerCase().equals("or")) {
			return first || second;
		}
		else {
			return first ^ second;
		}
	}
	
 	public void addToMeta(String strTableName, 
			String strClusteringKeyColumn,  
			Hashtable<String,String> htblColNameType) throws DBAppException {
		
		try {
			FileWriter outputfile = new FileWriter("metadata.csv", true); 
			// create CSVWriter object filewriter object as parameter 
		    CSVWriter writer = new CSVWriter(outputfile); 
		
			for (String x : htblColNameType.keySet()) {
				if (!(htblColNameType.get(x).equals("java.lang.Integer") || htblColNameType.get(x).equals("java.lang.Double") || htblColNameType.get(x).equals("java.lang.String"))) {
					throw new DBAppException("Invalid datatype");
				}
				String[] arr = new String[6];
				arr[0] = strTableName;
				arr[1] = x;
				arr[2] = htblColNameType.get(x);
				if (x.equals(strClusteringKeyColumn)) {
					arr[3] = "True";
				}
				else {
					arr[3] = "False";
				}
				arr[4] = "null";
				arr[5] = "null";
				writer.writeNext(arr, false);
				   
		         // closing writer connection 
		    }
			writer.close(); 
				
		}
		catch (IOException e) { 
		         // TODO Auto-generated catch block 
			e.printStackTrace(); 
		} 
	}

	public static void main( String[] args ){
	
//	
//	
//try{
//	    String strTableName = "Student";
//           DBApp	dbApp = new DBApp( );
////            dbApp.init();
////			Hashtable htblColNameType = new Hashtable( );
////			htblColNameType.put("id", "java.lang.Integer");
////			htblColNameType.put("name", "java.lang.String");
////			htblColNameType.put("gpa", "java.lang.Double");			
////			dbApp.createTable( strTableName, "id", htblColNameType );
//////
//			Hashtable htblColNameValue = new Hashtable( );
////			htblColNameValue.put("id", new Integer( 2343432 ));
////			htblColNameValue.put("name", new String("Ahmed Noor" ) );
////			htblColNameValue.put("gpa", new Double( 0.90 ) );			
////			dbApp.insertIntoTable( strTableName , htblColNameValue );
////			
////			Hashtable htblColNameValue2 = new Hashtable( );
////			htblColNameValue2.put("id", new Integer( 2343436 ));
////			htblColNameValue2.put("name", new String("Mo Noor" ) );
////			htblColNameValue2.put("gpa", new Double( 0.70 ) );
////			dbApp.insertIntoTable( strTableName , htblColNameValue2 );
////			
////			Hashtable htblColNameValue3 = new Hashtable( );
////			htblColNameValue3.put("id", new Integer( 2343434 ));
////			htblColNameValue3.put("name", new String("Hesham Noor" ) );
////			htblColNameValue3.put("gpa", new Double( 1.2 ) );
////			dbApp.insertIntoTable( strTableName , htblColNameValue3 );
////			
////			dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
//////			
//////	
//////			
////////			System.out.println("ID = " + " 2343436 "+b.search(0.70));
////////			System.out.println("ID = " + " 2343432 "+b.search(0.90));
//////			
//////			
////			Hashtable htblColNameValue4 = new Hashtable( );
////			htblColNameValue4.put("id", new Integer( 2343433 ));
////			htblColNameValue4.put("name", new String("Bibo Noor" ) );
////			htblColNameValue4.put("gpa", new Double( 1.4 ) );
////			dbApp.insertIntoTable( strTableName , htblColNameValue4 );
////			
////			Hashtable htblColNameValue5 = new Hashtable( );
////			htblColNameValue5.put("id", new Integer( 2343435 ));
////			htblColNameValue5.put("name", new String("Lamia Noor" ) );
////			htblColNameValue5.put("gpa", new Double( 1.4 ) );
////			dbApp.insertIntoTable( strTableName , htblColNameValue5 );
//////			
////			dbApp.createIndex( strTableName, "id", "idIndex" );
////			
////			dbApp.createIndex( strTableName, "name", "nameIndex" );
////			Hashtable htblColNameValue6 = new Hashtable( );
////			htblColNameValue6.put("name", new String("Bibo Noor" ) );
////			htblColNameValue6.put("gpa", new Double(1.4));
//////////		
////			
////			Hashtable htblColNameValue7 = new Hashtable( );
////			htblColNameValue7.put("gpa", new Double(0.7));
////			htblColNameValue7.put("name", new String("Zaky Noor" ) );
////			htblColNameValue7.put("id", new Integer( 78452));
////			dbApp.insertIntoTable(strTableName, htblColNameValue7);
//////			//dbApp.updateTable(strTableName, "2343432", htblColNameValue6);
//////			//BPlusTree b = BinaryFile.BinaryFileReaderTree(strTableName + "_" + "gpaIndex.class");
//
//////			
////			dbApp.deleteFromTable(strTableName, htblColNameValue6);
////			dbApp.deleteFromTable(strTableName, htblColNameValue7);
//////			
//////			p1 = BinaryFile.BinaryFileReader("Student1.class");
//////			p2 = BinaryFile.BinaryFileReader("Student2.class");
//////			
//////			System.out.println("post del "+p1 + "    " + p2);
////			//BPlusTree b1 = BinaryFile.BinaryFileReaderTree(strTableName + "_" + "gpaIndex.class");
////		    
//////			System.out.println("ID = " + " 2343434 "+b1.search(2343434));
//////			System.out.println("ID = " + " 2343436 "+b1.search(2343436));
//////			System.out.println("ID = " + " 2343432 "+b1.search(2343432));
//////			System.out.println("ID = " + " 2343434 " + b1.search(1.2));
//////			System.out.println(b1.search(1.4));
//////			System.out.println("newID = " + " 2343432 " + b1.search(1.4));
//////			System.out.println("ID = " + " 2343435 " + b1.search(1.4));
//////			System.out.println("ID = " + " 2343433 " + b1.search(1.4));
//////			System.out.println("oldID = " + " 2343432 " + b1.search(0.9));
//////			System.out.println("ID = " + "3ak" + b1.search(4.3));
//////			
//////			System.out.println();
////			
//////			System.out.println("ID = " + " 2343434 " + b2.search("Hesham Noor"));
//////			System.out.println("ID = " + " 2343436 " + b2.search("Mo Noor"));
//////			System.out.println("newID = " + " 2343432 " + b2.search(" Noor Ahmed"));
//////			System.out.println("ID = " + " 2343435 " + b2.search("Lamia Noor"));
//////			System.out.println("oldID = " + " 2343432 " + b2.search("Ahmed Noor"));
//////			
////			//dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
////			
//////			BPlusTree b = BinaryFile.BinaryFileReaderTree(strTableName + "_" + "gpaIndex.class");
//////			for (int i = 0; i < b.root.degree; i++) {
//////				System.out.print(b.root.keys[i] + ", ");
//////			}
//////			
//////			System.out.println();
////			
////			
////			
////			
//
////			htblColNameValue.clear( );
////			htblColNameValue.put("id", new Integer( 453455 ));
////			htblColNameValue.put("name", new String("Ahmed Noor" ) );
////			htblColNameValue.put("gpa", new Double( 0.95 ) );
////			dbApp.insertIntoTable( strTableName , htblColNameValue );
//////
////			htblColNameValue.clear( );
////			htblColNameValue.put("id", new Integer( 5674567 ));
////			htblColNameValue.put("name", new String("Dalia Noor" ) );
////			htblColNameValue.put("gpa", new Double( 1.25 ) );
////			dbApp.insertIntoTable( strTableName , htblColNameValue );
////////
////			htblColNameValue.clear( );
////			htblColNameValue.put("id", new Integer( 23498 ));
////			htblColNameValue.put("name", new String("John Noor" ) );
////			htblColNameValue.put("gpa", new Double( 1.5 ) );
////			dbApp.insertIntoTable( strTableName , htblColNameValue );
//////
////			htblColNameValue.clear( );
////			htblColNameValue.put("id", new Integer( 78452 ));
////			htblColNameValue.put("name", new String("Zaky Noor" ) );
////			htblColNameValue.put("gpa", new Double( 0.88 ) );
////			dbApp.insertIntoTable( strTableName , htblColNameValue );
//			
////			htblColNameValue.clear( );
////			htblColNameValue.put("id", new Integer(786));
////			htblColNameValue.put("name", new String("Ahmed Noor"));
////			htblColNameValue.put("gpa", new Double(6));
////			dbApp.insertIntoTable(strTableName, htblColNameValue);
//////			
////			htblColNameValue.clear( );
////			htblColNameValue.put("id", new Integer(99999999));
////			htblColNameValue.put("name", new String("Ahmed Noor"));
////			htblColNameValue.put("gpa", new Double(5));
////			dbApp.insertIntoTable(strTableName, htblColNameValue);
//////			
////			htblColNameValue.clear( );
////			htblColNameValue.put("id", new Integer(2000));
////			htblColNameValue.put("name", new String("Ahmed Noor"));
////			htblColNameValue.put("gpa", new Double(5));
////			dbApp.insertIntoTable(strTableName, htblColNameValue);
////			
////			dbApp.createIndex( strTableName, "name", "nameIndex" );
//			
//			htblColNameValue.clear( );
//			htblColNameValue.put("name", new String("Ahmed Noor"));
//			dbApp.deleteFromTable(strTableName, htblColNameValue);
////////           
//         Page p1 = BinaryFile.BinaryFileReader("Student1.class");
////
//			Table t = BinaryFile.BinaryFileReaderHashtable("tables.class").get("Student");
//           
//            BPlusTree b = BinaryFile.BinaryFileReaderTree("Student_nameIndex.class");
//           
//            System.out.println(b.search("Ahmed Noor"));
//////	
//           System.out.println(p1);
//			System.out.println(t.ranges);
//			
////			dbApp.deleteFromTable("Student", htblColNameValue);
////			
////			Page p2 = BinaryFile.BinaryFileReader("Student1.class");
////			//
////			Table t2 = BinaryFile.BinaryFileReaderHashtable("tables.class").get("Student");
////           
////            BPlusTree b2 = BinaryFile.BinaryFileReaderTree("Student_nameIndex.class");
////           
////            System.out.println(b2.search("Ahmed Noor"));
////////	
////            System.out.println(p2);
////			System.out.println(t2.ranges);
////
////          dbApp.createIndex( "Student", "name", "nameIndex" );
////          dbApp.createIndex( "Student", "id", "idIndex" );
////          dbApp.createIndex( "Student", "gpa", "gpaIndex" );
////			SQLTerm[] arrSQLTerms;
////			arrSQLTerms = new SQLTerm[2];
////			arrSQLTerms[0]=new SQLTerm();
////			arrSQLTerms[1]=new SQLTerm();
//////			arrSQLTerms[2]=new SQLTerm();
////			arrSQLTerms[0]._strTableName =  "Student";
////			arrSQLTerms[0]._strColumnName=  "name";
////			arrSQLTerms[0]._strOperator  =  "=";
////			arrSQLTerms[0]._objValue     =  "Ahmed Noor";
//////			
////			arrSQLTerms[1]._strTableName =  "Student";
////			arrSQLTerms[1]._strColumnName=  "gpa";
////			arrSQLTerms[1]._strOperator  =  "<=";
////			arrSQLTerms[1]._objValue     = 5.0;
//////			arrSQLTerms[1]._strTableName =  "Student";
//////			arrSQLTerms[1]._strColumnName=  "gpa";
//////			arrSQLTerms[1]._strOperator  =  "=";
//////			arrSQLTerms[1]._objValue     =  new Double( 1.5 );
////			
////			arrSQLTerms[2]._strTableName =  "Student";
////			arrSQLTerms[2]._strColumnName=  "name";
////			arrSQLTerms[2]._strOperator  =  "<=";
////			arrSQLTerms[2]._objValue     = "Lamia Noor";
////
////			String[]strarrOperators = new String[1];
////			strarrOperators[0] = "xor";
//////			strarrOperators[1] = "xor";
////			// select * from Student where name = "John Noor" or gpa = 1.5;
////			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
////            while (resultSet.hasNext()) {
////            	System.out.println(resultSet.next());
////            	
////			}
//	}
//		catch(Exception exp){
//			exp.printStackTrace( );
//		}
//	
////		try {
////			System.out.println(evaluateBooleanExpression(new boolean[] {false, false, true}, new String[] {"or", "and"}));
////		} catch (DBAppException e) {
////			// TODO Auto-generated catch block
////			e.printStackTrace();
////		}
////		Tuple t = new Tuple(3);
////		t.row[0] = new String("Rahaf!!!!!!!");
////		t.row[1] = new Integer(34);
////		t.row[2] = new Double(3.14);
////		
////		SQLTerm term = new SQLTerm();
////		term._strTableName =  "Student";
////		term._strColumnName=  "name";
////		term._strOperator  =  "!=";
////		term._objValue     =  3.12;
////		
////		Hashtable<String, Integer> ht = new Hashtable<>();
////		ht.put("name", 2);
////		
////		System.out.println(evaluateCondition(t, term, ht));
//	
	}
}