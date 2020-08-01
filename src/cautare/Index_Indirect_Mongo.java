package cautare;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;

//indexul invers cu stocarea in baza de date Mongo DB

public class Index_Indirect_Mongo {

	//hashtable pentru indexul invers la nivel de fisier
	 private static HashMap<String, HashMap<String, Integer>> ReverseIndex;
	 //hashtable pentru fisierul total de indexare inversa
	 private static HashMap<String, HashMap<String, Integer>> ReverseTotal;
	 
	//obiecte folositoare in lucru cun baza de date MongoDB
	MongoClient mongoClientRev;
	MongoDatabase databaseRev;
	MongoCollection<Document> collectionRev = null;
	MongoCollection<Document> collectionDir;
	Document wordMapper;
	int nrFiles;
	
	 public Index_Indirect_Mongo() 
	 { 
		//creem conexiunea cu mongo
		 mongoClientRev = new MongoClient ("localhost", 27017);
		
		//aducem baza de date intr-un obiect java
		databaseRev = mongoClientRev.getDatabase("Indexare");
		
		//aducem colectia care contine indexul direct intr-un obiect java
		collectionRev = databaseRev.getCollection("IndexInvers");
		//stergem tot ce este in interiorul colectiei
		collectionRev.drop();
		
		//creem din nou colectia
		databaseRev.createCollection("IndexInvers");
		collectionRev = databaseRev.getCollection("IndexInvers");
		
		//luam indexul direct din baza de date
		collectionDir = databaseRev.getCollection("IndexDirect");
		 
		//instantiem obiectele de tip HashMap
		ReverseIndex = new HashMap<String, HashMap<String,Integer>>();
		ReverseTotal = new HashMap<String, HashMap<String,Integer>>();
		
		//instantiem documentul pentru maparea inversa
		wordMapper = new Document();
		
		//nr total de documente (pentru idf)
		//Index_Direct_Mongo idm = new Index_Direct_Mongo();
		nrFiles = Index_Direct_Mongo.getNrOfFiles();
	 }
	
	//functia care proceseaza un fiser de tip index direct si ii inverseaza valorile
	public void CreatingHashMapReverse(String docId)
	{		
		System.out.print("...   ");
		
		try {			
			//preluam documentul de parametru dat
			Document indDirDoc = collectionDir.find(eq("_id", docId)).first();		
			
			//parcurgem toate obiectele din interior
			for(int i=0; i<indDirDoc.size()-1;i++)
			{
				//preluam pe rand cate un obiect
				Document iterator = (Document) indDirDoc.get("Fisier"+i);
				
				//preluam setul de cuvinte cheie
				Document keywords = (Document) iterator.get("Keywords");
				
				Set<Entry<String, Object>> keys = keywords.entrySet();
				
				Iterator iter = keys.iterator();
				//iteram setul de cuvinte cheie
				while (iter.hasNext()) 
				{
					//preluam cheie: valoare 
					Entry entry = (Entry)iter.next();

					//se formeaza indexul invers pentru fisierul curent
					if(ReverseIndex.containsKey(entry.getKey()) == false)
					{
						Integer val = (Integer) entry.getValue();
						String key = (String) entry.getKey();
						HashMap<String, Integer> ht1 = new HashMap<String, Integer>();
						ht1.put((String) iterator.get("Fisier_intrare"), val);
						ReverseIndex.put((String) key, ht1);
					}
					else
					{
						Integer val = (Integer) entry.getValue();
						String key = (String) entry.getKey();
						HashMap<String, Integer> ht2 = new HashMap<String, Integer>();
						ht2 = ReverseIndex.get(key);
						ht2.put((String) iterator.get("Fisier_intrare"), val);
						ReverseIndex.replace(key, ht2);
					}
					
					//se creeaza indexul invers total
					if(ReverseTotal.containsKey(entry.getKey()) == false)
					{
						Integer val = (Integer) entry.getValue();
						String key = (String) entry.getKey();
						HashMap<String, Integer> ht1 = new HashMap<String, Integer>();
						ht1.put((String) iterator.get("Fisier_intrare"), val);
						ReverseTotal.put((String) key, ht1);
					}
					else
					{
						Integer val = (Integer) entry.getValue();
						String key = (String) entry.getKey();
						HashMap<String, Integer> ht2 = new HashMap<String, Integer>();
						ht2 = ReverseTotal.get(key);
						ht2.put((String) iterator.get("Fisier_intrare"), val);
						ReverseTotal.replace(key, ht2);
					}
					//preluam cheie: valoare ca sa scapam de tf
					entry = (Entry)iter.next();
				}
			}

        }catch(NullPointerException ex)
		{
        	ex.printStackTrace();
		}
		catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	//functia care proceseaza lista de fisiere din directorul corespunzator indexului direct
	public void ProcessingFilesReverse()
	{
		System.out.println("Se incepe crearea indexului invers ...");
		
		MongoCursor<Document> cursor = collectionDir.find().iterator();
		try {
		    while (cursor.hasNext()) 
		    {
		    	//preluam numele directorului din indexul direct
		    	String numeFolder = cursor.next().getString("_id");
		    	
		    	if(!numeFolder.contentEquals("Document_de_Mapare_Directa"))
		    	{
		    		//creem hashMap-ul
			    	CreatingHashMapReverse(numeFolder);
			    	
			    	//scriem documentul
			    	numeFolder = numeFolder.substring(0, numeFolder.length()-13) + "_index_invers";
			    	WriteReverseIndexFile(numeFolder);
			    	
			    	//curatam hashMap-ul per document
			    	ReverseIndex.clear();
		    	}
		    }
		} finally {
		    cursor.close();
		}
			
		try {
			//creem fisierul final de index invers
			String indName = "Reverse_Index_Total"; 
			
			//creem documentele necesare
			Document hashMapDoc = new Document();
			Document hashMapDocTotal = new Document();
			Document elemHashMap = new Document();
			
			//creem un iterator si scriem in fisierul final
			Iterator iter = ReverseTotal.entrySet().iterator();
			while(iter.hasNext())
			{
				Map.Entry value = (Map.Entry) iter.next();
				//scriem rand pe rand cate un element din hashMap
				elemHashMap = WriteDocumentTotal((HashMap<String, Integer>) value.getValue());
				
				hashMapDoc.append((String) value.getKey(), elemHashMap);
				
				//scriem in fisierul de mapare
				wordMapper.append((String) value.getKey(), indName);
			}
			//documentul final de indexare inversa
			hashMapDocTotal.append("_id", indName);
			hashMapDocTotal.append("keywords", hashMapDoc);
			collectionRev.insertOne(hashMapDocTotal);
			
			//documentul mapper
			Document dcmMapperDocument = new Document();
			dcmMapperDocument.append("_id", "Document_de_Mapare_Inversa");
			dcmMapperDocument.append("Colectie", "IndexInvers");
			dcmMapperDocument.append("Content", wordMapper);	
			collectionRev.insertOne(dcmMapperDocument);
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		System.out.println("\nIndexul invers s-a realizat cu succes!");

	}
	
	//functia care scrie un document in colectia de documente a indexului invers
	public void WriteReverseIndexFile(String nameFolder)
	{
		try {
			//creem documentele necesare
			Document hashMapDoc = new Document();
			Document hashMapDocTotal = new Document();
			Document elemHashMap = new Document();
			
			//creem un iterator si parcurgem HashMap-ul
			Iterator iter = ReverseIndex.entrySet().iterator();
			while(iter.hasNext())
			{
				Map.Entry value = (Map.Entry) iter.next();
				//System.out.println(value.getKey() + ": " + value.getValue());
				
				elemHashMap = WriteDocument((HashMap<String, Integer>) value.getValue());
				
				hashMapDoc.append((String) value.getKey(), elemHashMap);
			}
			
			//formama documentul final de inserat
			hashMapDocTotal.append("_id", nameFolder);
			hashMapDocTotal.append("keywords", hashMapDoc);
			
			
			//adaugam documentul la colectia de documente
			collectionRev.insertOne(hashMapDocTotal);
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
	}
	
	//functia care returneaza un Document cu HashNap-ul din interior
	public Document WriteDocument(HashMap<String, Integer> hm)
	{
		//creem un document nou
		Document doc = new Document();
		
		try {
			int i=0;
			//se introduc datele din HashTable in Document
			for(Entry<String, Integer> entry : hm.entrySet())
			{
				doc.append("File" + i, entry.getKey());
				doc.append("Count" + i,entry.getValue());
				i++;
			}
			
			return doc;
		}catch(Exception e) {
			e.printStackTrace();
			return doc;
		}
	}
	
	//functia care returneaza un Document cu HashNap-ul din interior
	public Document WriteDocumentTotal(HashMap<String, Integer> hm)
	{
		//creem un document nou
		Document doc = new Document();
		
		try {
			int i=0;
			//se introduc datele din HashTable in Document
			for(Entry<String, Integer> entry : hm.entrySet())
			{
				doc.append("File" + i, entry.getKey());
				doc.append("Count" + i,entry.getValue());
				i++;
			}
			
			//calculam idf si il inseram in document
			int aux = 1 + i;
			double clc = precisionCompute(nrFiles, aux, 4);
			float idf = (float) Math.log(clc);
			if(idf<0)
			{
				idf = 0;
			}
			doc.append("idf", idf);
			
			return doc;
		}catch(Exception e) {
			e.printStackTrace();
			return doc;
		}
	}
	
		public double precisionCompute(int x, int y, int n) 
	    { 
	        // Base cases 
	        if (y == 0) { 
	            return 0; 
	        } 
	        if (x == 0) { 
	            return 0; 
	        }
	  
	        // Integral division 
	        int d = x / y; 
	  
	        String result = "";

	        for (int i = 0; i <= n; i++) 
	        { 
	        	result +=d;
	            x = x - (y * d);
	            if (x == 0) 
	                break; 
	            x = x * 10; 
	            d = x / y; 
	            if (i == 0) 
	            {
	            	result+=".";
	            }
	        }
	        double resDouble = Double.parseDouble(result);
	        return resDouble;
	    } 
		
	public void printCollection()
	{
		MongoCursor<Document> cursor = collectionRev.find().iterator();
		try {
		    while (cursor.hasNext()) 
		    {
		        System.out.println(cursor.next().toJson());
		    }
		} finally {
		    cursor.close();
		}
	}
	
}
