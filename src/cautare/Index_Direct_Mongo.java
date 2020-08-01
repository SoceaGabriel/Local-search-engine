package cautare;

import java.io.BufferedReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Map.Entry;

import org.bson.Document;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

//indexul direct cu stocarea in baza de date Mongo DB

public class Index_Direct_Mongo {

	//hash table pentru stocarea cuvintelor si nr de apritii ale lor
	private Hashtable<String, Integer> table = new Hashtable<String, Integer>();
	
	//lista folderelor de parcurs
	private Queue<File> dirList;
	private Queue<File> filesList;
	
	//obiecte folositoare in lucru cun baza de date MongoDB
	private MongoClient mongoClient;
	private MongoDatabase database;
	private MongoCollection<Document> collection = null;
	
	private static int nrOfFiles;
	
	//constructorul fara parametri
	public Index_Direct_Mongo() 
	{
		//creem conexiunea cu mongo
		mongoClient = new MongoClient ("localhost", 27017);
		
		//aducem baza de date intr-un obiect java
		database = mongoClient.getDatabase("Indexare");
		
		//aducem colectia care contine indexul direct intr-un obiect java
		collection = database.getCollection("IndexDirect");
		//stergem tot ce este in interiorul colectiei
		collection.drop();
		
		//creem din nou colectia
		database.createCollection("IndexDirect");
		collection = database.getCollection("IndexDirect");
		
		//initializam listele folosite
		dirList = new LinkedList<>();
		filesList = new LinkedList<File>();
		
		nrOfFiles = 0;
	}

	public static int getNrOfFiles()
	{
		return nrOfFiles;
	}
	//functia care returneaza un buuffered reader pe baza unui fisier dat ca parametru
	public BufferedReader OpenFile(String name)
	{
		try
		{
			File file = new File(name);
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			return br;
		}catch (IOException e) {
            e.printStackTrace();
            System.out.println("Index direct: Nu s-a putut deschide fisierul de intrare!");
            BufferedReader br = null;
            return br;
        }
	}
	
	//functia care verifica daca un cuvant este o exceptie
	public boolean isException(String word)
	{
		//deschidem fisierul de exceptii sub forma unui buffer
		BufferedReader brException = OpenFile("Extra_Dir\\Exceptions.txt");
		try {
			//string in care citim fiecare cuvant din fisier
			String ExWord = brException.readLine();
			
			while(ExWord!=null)
			{
				//verificam daca cuvantul se gaseste in lista de exceptii
				if(word.toUpperCase().contentEquals(ExWord.toUpperCase()))
				{
					return true;
				}
				ExWord = brException.readLine();
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	//functia care verifica daca un cuvant este stopWord
	public boolean isStopWord(String word)
	{
		//deschidem fisierul de exceptii sub forma unui buffer
		BufferedReader brStopWord = OpenFile("Extra_Dir\\StopWords.txt");
		
		try {
			//string in care citim fiecare cuvant din fisier
			String StopWord = brStopWord.readLine();
			
			while(StopWord!=null)
			{
				//verificare cuvantul dat ca parametru se gaseste in lista de stopwords
				if(word.toUpperCase().contentEquals(StopWord.toUpperCase()))
				{
					return true;
				}
				StopWord = brStopWord.readLine();
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
		return false;
	}	
	
	//functia care creaza un hashtable
	public void CreatingHashTable(String FileNameFromFolder)
	{
		BufferedReader br = OpenFile(FileNameFromFolder);
		int c = 0;             
	    String words="";
	    Porter_Stemmer ps = new Porter_Stemmer();
	    String result="";
	    
	    try
		{
	    	while((c = br.read()) != -1)
		    {
	    		char ch = (char) c;
		        if((ch >= 'A' && ch<='Z') || (ch>='a' && ch<='z'))
		        {
		        	//compunere cuvant
		        	words+=ch;
		        }
		        else
		        {
		        	if(words.length() >= 1)
		        	{
		        		//procesare cuvant
		        		if(isException(words) == true)
		        		{
		        			//aici se contorizeaza aparitia cuvantului in HashTable
	        				Integer count = table.get(words);
				            table.put(words, count == null ? 1 : (count+1));
		        		}
		        		else if(isStopWord(words)==true)
		        		{
		        			//se trece peste
		        			words = "";
		        			continue;
		        		}
		        		else
		        		{
		        			//aducem cuvantul la forma canonica
		        			result = ps.PorterAlgoritm(words);
	        				//aici se contorizeaza aparitia cuvantului in HashTable
	        				Integer count = table.get(result);
				            table.put(result, count == null ? 1 : (count+1));
				            result="";
		        		}
		        	}
		        	words = "";
		        }
		    }
	    	//inchidem bufferul din fisierul de citire
	        br.close();
		}catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	//functia care proceseaza toate fisierele
	public void ProcessingFiles(String NumeFisier)
	{	
		
		try {			
			Document total = new Document();
			
			//adaugam ca identificator numele folderului respectiv + _index_direct => numeFisier
			total.append("_id", NumeFisier);
			int i=0;
			
			//parcurgem lista de fisiere si facem procesarile necesare
			for(File crtFile : filesList)
			{
				System.out.print("...   ");
				//creem HashTable-ul pt fisierul curent
				CreatingHashTable(crtFile.getAbsolutePath());
				
				//creare nume pentru document per fisier de intrare
				String fil = crtFile.getAbsolutePath();
				
				//creem un obiect in care o sa punem hashtable-ul si inca un obiect in care concatenam numele fisierului si hashtable-ul
				Document doc = WriteInMongo();
				Document docTotal = new Document();
				
				//punem numnele fisierului de intrare in document
				docTotal.append("Fisier_intrare", fil);
				
		        //punem documentul corespunzator hashtable-ului in doc curent
				docTotal.append("Keywords", doc);
		        
				//punem documentul 
				total.append("Fisier" + i, docTotal);

				i++;
			}
			//inseram documentul total
			collection.insertOne(total);
			
		}catch(Exception e) {
			e.printStackTrace();
		}finally{
			try {				
				//stergem lista de fisiere ca in continuare sa le procesam doar pe cele care nu au fost procesate
				filesList.clear();
				
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void CrossingDirectors(File path)
	{	
		System.out.println("Se incepe crearea indexului direct ...");
		
		Document doc = new Document();
		Document mapperDoc = new Document();
		int i = 0;
		
		//se adauga directorul cu calea specificata in lista de directoare
		dirList.add(path);
		
		//parcurgem structura de fisiere si directoare
		while(!dirList.isEmpty())
		{
			//scoatem ultimul director adaugat in lista
			File f = dirList.poll();
			//creem o lista de fisiere corespunzatoare directorului curent
			File[] filesL = f.listFiles();
			
			//construim numele fisierului de indexare directa pentru acest folder
			String nameF = f.getName() + "_index_direct";
			
			//parcurgem lista de fisiere
			for(File fis : filesL)
			{
				if(fis.isDirectory())
				{
					dirList.add(fis);
				}
				else
				{
					//incrementam nr de documente si adaugam numele fisierului in fileList
					nrOfFiles++;
					filesList.add(fis);
					
					//creem documentul mapper in care asociem fiecarui nume de fisier numele documenttului in care se afla
					doc.append("Fisier_intrare_" + i, fis.getAbsolutePath());
					doc.append("Document_index_direct_" + i, nameF);
					i++;
				}
			}
			
			//dupa ce am extras toate fisierele din folderul curent, le procesam
			ProcessingFiles(nameF);
		}
		
		//scriem informatiile necesare in mapper si le inseram in colectie
		mapperDoc.append("_id", "Document_de_Mapare_Directa");
		mapperDoc.append("Colectie", "IndexDirect");
		mapperDoc.append("Content", doc);
		collection.insertOne(mapperDoc);
		
		System.out.println("\nIndexul direct s-a realizat cu succes!");
	}
	
	//functia care calculeaza o impartire cu un nr de zecimale impus
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
	
	//functia care returneaza un Document cu HashTable-ul creat pentru fisierul respectiv
	public Document WriteInMongo()
	{
		//creem un Array Json
		Document doc = new Document();
		
		try {
			//se introduc datele din HashTable in document
			for(Entry<String, Integer> entry : table.entrySet())
			{
				//System.out.println(entry.getKey() + "\t" + entry.getValue());
				doc.append(entry.getKey(), entry.getValue());
				double result = precisionCompute(entry.getValue(), table.size(), 6);
				doc.append("tf_" + entry.getKey(), result);
			}
			return doc;
		}catch(Exception e) {
			e.printStackTrace();
			return doc;
		}finally{
			try {
				//stergem datele din HashTable pentru a fi liber pentru urmatorul fisier care trebuie procesat
				table.clear();
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	//functia care afiseaza hashtable-ul
	public void afisare()
	{
		try {
			for(Entry<String, Integer> entry : table.entrySet())
			{
				System.out.println(entry.getKey() + "\t" + entry.getValue());
			}
		}catch(Exception e) {
			e.printStackTrace();
			System.out.println("Nu s-a putut realiza afisarea!");
		}
	}
	
	//functia care afiseaza colectia de documente
	public void printCollection()
	{
		MongoCursor<Document> cursor = collection.find().iterator();
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
