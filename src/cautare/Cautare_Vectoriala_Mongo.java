package cautare;

import static com.mongodb.client.model.Filters.eq;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class Cautare_Vectoriala_Mongo {

	private ArrayList<String> keywords;
	private ArrayList<String> operators;
	private HashMap<String, ArrayList<String>> fileList;
	private ArrayList<String> uniqueFileList;
	private double[] cosSimScore;
	
	//obiecte necesare in lucru cu baza de date
	MongoClient mongoClient;
	MongoDatabase database;
	MongoCollection<Document> collection = null;
	MongoCollection<Document> collectionDirect = null;
	
	//constructorul fara parametri
	public Cautare_Vectoriala_Mongo()
	{
		//instantiem listele folosite in cautare
		keywords = new ArrayList<String>();
		operators = new ArrayList<String>();
		fileList = new HashMap<String, ArrayList<String>>();
		uniqueFileList = new ArrayList<String>();
		//instantiem obiectele necesare lucrului cu bd
		//creem conexiunea cu mongo
		mongoClient = new MongoClient ("localhost", 27017);
		
		//aducem baza de date intr-un obiect java
		database = mongoClient.getDatabase("Indexare");
		
		//aducem colectia care contine indexul direct intr-un obiect java
		collection = database.getCollection("IndexInvers");
		collectionDirect = database.getCollection("IndexDirect");
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
            System.out.println("Cautarea vectoriala: Nu s-a putut deschide fisierul de intrare!");
            BufferedReader br = null;
            return br;
        }
	}
	
	//functia care verifica daca un cuvant este o exceptie
	public boolean isExceptionBR(String word)
	{
		//deschidem fisierul de exceptii sub forma unui buffer
		BufferedReader brException = OpenFile("Extra_Dir\\Exceptions.txt");
		try {
			//string in care citim fiecare cuvant din fisier
			String ExWord = brException.readLine();
			
			while(ExWord!=null)
			{
				//verificam daca cuvantul se gaseste in lista de exceptii
				if(word.contentEquals(ExWord))
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
	public boolean isStopWordBR(String word)
	{
		//deschidem fisierul de exceptii sub forma unui buffer
		BufferedReader brStopWord = OpenFile("Extra_Dir\\StopWords.txt");
		
		try {
			//string in care citim fiecare cuvant din fisier
			String StopWord = brStopWord.readLine();
			
			while(StopWord!=null)
			{
				//verificare cuvantul dat ca parametru se gaseste in lista de stopwords
				if(word.contentEquals(StopWord))
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
	
	public void processSearchWords(String SearchWords)
	{
		String[] arrOfStr = SearchWords.split(" ");
		
		 // + pentru operatia AND
		 // - pentru operatia OR
		 // ~ pentru operatia NOT	
		
		//parcurgem lista impartita in cuvinte si operatori
		for(int i=0; i<arrOfStr.length; i++)
		{
			if((arrOfStr[i].contains("+") || arrOfStr[i].contains("-") || arrOfStr[i].contains("~")) && !isStopWordBR(arrOfStr[i+1])){
				
				operators.add(arrOfStr[i]);
				
			}else if(isExceptionBR(arrOfStr[i])==true){
				
				keywords.add(arrOfStr[i]);
				
			}else if(isStopWordBR(arrOfStr[i]) || arrOfStr[i].contains("+") || arrOfStr[i].contains("-") || arrOfStr[i].contains("~")){
				
				continue;
				
			}else{
				String canonicForm = "";
				Porter_Stemmer ps = new Porter_Stemmer();
				canonicForm =  ps.PorterAlgoritm(arrOfStr[i]);
				keywords.add(canonicForm);
			}
		}
		instantiationLists();
	}
	
	//se preia lista de documente corespunzatoare fiecarui cuvant din indexul invers din baza de date MongoDB
	public void instantiationLists()
	{
		try {
			//aducem documentul total care comtine indexul invers
			Document indexIndirect = collection.find(eq("_id", "Reverse_Index_Total")).first();		
			
			for(int i=0; i<keywords.size(); i++)
			{
				ArrayList<String> listOfFiles = new ArrayList<String>();
				Document kwds = (Document) indexIndirect.get("keywords");
				Set<Entry<String, Object>> keys = kwds.entrySet();
				Iterator iter = keys.iterator();
				//iteram setul de cuvinte cheie si liste de fisiere
				while (iter.hasNext()) 
				{
					//preluam cheie: valoare 
					Entry entry = (Entry)iter.next();
					if(entry.getKey().equals(keywords.get(i)))
					{
						Document fileSplit = (Document) kwds.get(entry.getKey());
						for(int j=0; j<fileSplit.size(); j++)
						{
							if(fileSplit.containsKey("File"+j))
							{
								listOfFiles.add((String) fileSplit.get("File"+j));
								if(!uniqueFileList.contains(fileSplit.get("File"+j)))
								{
									uniqueFileList.add((String) fileSplit.get("File"+j));
								}
							}
						}
						fileList.put((String) entry.getKey(), listOfFiles);
					}
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void initializeSimCosVec()
	{
		cosSimScore = new double[uniqueFileList.size()];
		for(int i=0; i<uniqueFileList.size(); i++)
		{
			cosSimScore[i] = 0;
		}
	}
	
	//functia care calculeaza similaritatea cosinus pentru fiecare document in care apare cel putin un cuvant din query-ul de cautare
	public void VectorialSearch()
	{
		initializeSimCosVec();
		for(int i=0; i<keywords.size(); i++)
		{
			for(int j = 0; j< uniqueFileList.size(); j++)
			{
				cosSimScore[j] += round((MultiplyVectors(keywords.get(i), uniqueFileList.get(j))), 4);
			}
		}
		
		for(int k=0; k<uniqueFileList.size(); k++)
		{
			cosSimScore[k] = round(cosSimScore[k] / DocumentNorm(uniqueFileList.get(k)) * SearchQueryNorm(), 4);
		}
		
		sortDocuments();
	}
	
	public void sortDocuments()
	{
		//mutam numele fisierelor din uniqueList intr-un vector de string auxiliar
		String[] auxFileList = new String[uniqueFileList.size()];
		for(int count=0; count<uniqueFileList.size(); count++)
		{
			auxFileList[count] = uniqueFileList.get(count);
		}
		
		//ordonam elementele descrescator in functie de relevanta
		for(int i=0;i<cosSimScore.length-1; i++)
		{
			for(int j=i+1; j<cosSimScore.length; j++)
			{
				if(cosSimScore[i]<cosSimScore[j])
				{
					//interschimbam valorile de similaritate
					double aux = cosSimScore[i];
					cosSimScore[i] = cosSimScore[j];
					cosSimScore[j] = aux;
					
					//interschimbam documentele
					String aux_string = auxFileList[i];
					auxFileList[i] = auxFileList[j];
					auxFileList[j] = aux_string;
				}
			}
		}
		
		//mutam din nou elementele in array
		uniqueFileList.clear();
		for(int k=0; k<auxFileList.length; k++)
		{
			uniqueFileList.add(auxFileList[k]);
		}
	}
	
	
	//DocumentNorm
	public double DocumentNorm(String docName)
	{
		int auxResult = 0;
		
		MongoCursor<Document> cursor = collectionDirect.find().iterator();
		//aducem documentul total care comtine indexul invers
		
		while(cursor.hasNext())
		{
	    	//preluam numele directorului din indexul direct
	    	String numeFolder = cursor.next().getString("_id");
	    	if(!numeFolder.contentEquals("Document_de_Mapare_Directa"))
	    	{
	    		Document indDirDoc = collectionDirect.find(eq("_id", numeFolder)).first();
	    		for(int i=0; i<indDirDoc.size()-1;i++)
	    		{
	    			//preluam pe rand cate un obiect
					Document iterator = (Document) indDirDoc.get("Fisier"+i);
					String entryFile = (String) iterator.get("Fisier_intrare");
					if(entryFile.equals(docName))
					{
						Document keywordList = (Document) iterator.get("Keywords");
						Set<Entry<String, Object>> keys = keywordList.entrySet();
						Iterator iter = keys.iterator();
						//iteram setul de cuvinte cheie si liste de fisiere
						while (iter.hasNext()) 
						{
							Entry entry = (Entry)iter.next();
							int count = (int) entry.getValue();
							auxResult +=  count * count;
							
							entry = (Entry)iter.next();
						}
						return round(Math.sqrt(auxResult),4); 
					}
	    		}
	    	}
		}	
		return 0.0;
	}
	
	//SearchQueryNorm
	public double SearchQueryNorm()
	{
		int suma =0;
		int count = 0;
		for(int i=0; i<keywords.size(); i++)
		{
			for(int j=0; j<keywords.size(); j++)
			{
				if(keywords.get(i).contentEquals(keywords.get(j))) count++;
			}
			suma += Math.pow(count, 2);
			count = 0;
		}
		return round(Math.sqrt(suma), 4);
	}
	
	//MultiplyVectors
	public double MultiplyVectors(String word, String docName)
	{
		double tf = 0, idf = 0;
		//scoatem tf din baza de date
		MongoCursor<Document> cursor = collectionDirect.find().iterator();
		
		//aducem documentul total care comtine indexul invers
		while(cursor.hasNext())
		{
	    	//preluam numele directorului din indexul direct
	    	String numeFolder = cursor.next().getString("_id");
	    	if(!numeFolder.contentEquals("Document_de_Mapare_Directa"))
	    	{
	    		Document indDirDoc = collectionDirect.find(eq("_id", numeFolder)).first();
	    		for(int i=0; i<indDirDoc.size()-1;i++)
	    		{
	    			//preluam pe rand cate un obiect
					Document iterator = (Document) indDirDoc.get("Fisier"+i);
					String entryFile = (String) iterator.get("Fisier_intrare");
					if(entryFile.equals(docName))
					{
						Document keywordList = (Document) iterator.get("Keywords");
						Set<Entry<String, Object>> keys = keywordList.entrySet();
						Iterator iter = keys.iterator();
						//iteram setul de cuvinte cheie si liste de fisiere
						while (iter.hasNext()) 
						{
							Entry entry = (Entry)iter.next();
							
							if(entry.getKey().equals("tf_" + word))
							{
								tf = (double) entry.getValue();
							}	
						}
					}
	    		}
	    	}
		}	
		
		//scoatem idf din baza de date
		Document indexIndirect = collection.find(eq("_id", "Reverse_Index_Total")).first();		
		
		Document kwds = (Document) indexIndirect.get("keywords");
		Set<Entry<String, Object>> keys = kwds.entrySet();
		Iterator iter = keys.iterator();
		//iteram setul de cuvinte cheie si liste de fisiere
		while (iter.hasNext()) 
		{
			//preluam cheie: valoare 
			Entry entry = (Entry)iter.next();
			
			if(entry.getKey().equals(word))
			{
				Document wordFiles = (Document) entry.getValue();
				idf = (double) wordFiles.get("idf");
			}
		}
		return round((tf*idf),4);
	}
	
	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    long factor = (long) Math.pow(10, places);
	    value = value * factor;
	    long tmp = Math.round(value);
	    return (double) tmp / factor;
	}
	
	//functia care aplica operatiile in ordinea aparitiilor lor
	public ArrayList<String> applyOperators()
	{
		ArrayList<String> finalList = new ArrayList<String>();
		ArrayList<String> auxList = new ArrayList<String>();
		
		//efectuam prima operatie folosind functiile cu 2 parametri String
		switch(operators.get(0))
		{
			case "+":
				auxList = listOfAND(keywords.get(0), keywords.get(1));
				break;
			case "-":
				auxList = listOfOR(keywords.get(0), keywords.get(1));
				break;
			case "~":
				auxList = listOfNOT(keywords.get(0), keywords.get(1));
				break;
		}
		finalList = auxList;
		//daca avem mai multe operatii de facut
		if(keywords.size() >= 3)
		{
			int j=1;
			for(int i=2; i<keywords.size(); i++)
			{
				//verificam ce operator avem si aplicam operatia respectiva aplicand functiile supraincarcate
				switch(operators.get(j))
				{
					case "+":
						finalList = listOfAND(auxList, keywords.get(i));
						//lista auxiliara devine lista de la iteratia curenta si va fi folosita din nou la iteratia urmatoare
						auxList = finalList;
						break;
						
					case "-":
						ArrayList<String> or = new ArrayList<String>();
						or = listOfOR(auxList, keywords.get(i));
						finalList.addAll(or);
						auxList = finalList;
						break;
						
					case "~":
						finalList = listOfNOT(auxList, keywords.get(i));
						auxList = finalList;
						break;
				}
				j++;
			}
			return finalList;
		}
		else
		{
			return auxList;
		}
		
	}
	
	//functia care afiseaza lista finala de docomente
	public void printFinalList()
	{
		VectorialSearch();
		
		ArrayList<String> finalList = new ArrayList<String>();
		
		System.out.println(keywords);
		System.out.println(operators);
		
		//aplicam toate operatiile
		finalList = applyOperators();
		if(finalList.isEmpty())
		{
			System.out.println("Nu s-au gasit pagini care sa indeplineasca criteriile cerute!");
		}
		else
		{
			for(int k=0; k<uniqueFileList.size(); k++)
			{
				if(!finalList.contains(uniqueFileList.get(k))) 
				{
					uniqueFileList.remove(uniqueFileList.get(k));
					for(int j=k+1; j<cosSimScore.length; j++)
					{
						cosSimScore[j-1] = cosSimScore[j];
					}
				}
			}
			
			System.out.println("Lista de documente care satisface cautarea utilizatorului: ");
			for(int i=0; i<uniqueFileList.size(); i++)
			{
				System.out.println(uniqueFileList.get(i) + " - cu scorul de relevanta: " + cosSimScore[i]);
			}
		}
	}
	
	//functii pentru operatii (AND, SAU, NOT)
	//functia care returneaza lista cu intersectia de fisiere
	public ArrayList<String> listOfAND(String word1, String word2)
	{
		ArrayList<String> filesAND = new ArrayList<String>();
		
		//parcurgem lista de fisiere a cuvantului 1
		for(String s1 : fileList.get(word1))
		{
			//parcurgem lista de fisiere a cuvantului 2
			for(String s2 : fileList.get(word2))
			{
				if(s2.contains(s1) && (filesAND.contains(s1) == false))
				{
					//daca caile spre cele 2 fisiere sunt identice si calea nu mai exista in lista care se creeaza, atunci se adauga
					filesAND.add(s1);
				}
			}
		}
		return filesAND;
	}
	
	//functia care returneaza lista cu intersectia de fisiere - varianta supraincarcata
	public ArrayList<String> listOfAND(ArrayList<String> list1, String word2)
	{
		ArrayList<String> filesAND = new ArrayList<String>();
		
		//parcurgem lista de fisiere a cuvantului 1
		for(String s1 : list1)
		{
			//parcurgem lista de fisiere a cuvantului 2
			for(String s2 : fileList.get(word2))
			{
				if(s1.contains(s2) && (filesAND.contains(s1) == false))
				{
					//daca caile spre cele 2 fisiere sunt identice si calea nu mai exista in lista care se creeaza, atunci se adauga
					filesAND.add(s1);
				}
			}
		}
		return filesAND;
	}
	
	//functia care returneaza lista cu reuniunea de fisiere
	public ArrayList<String> listOfOR(String word1, String word2)
	{
		ArrayList<String> filesOR = new ArrayList<String>();
		
		for(String s1 : fileList.get(word1))
		{
			//punem toate fisierele din lista cuvantului 1 in lista care se creeaza
			filesOR.add(s1);
		}
		
		//punem restul de fisiere care nu au fost adaugate in lista
		for(String s2 : fileList.get(word2))
		{
			
			if(filesOR.contains(s2) == false)
			{
				filesOR.add(s2);
			}
		}
		
		return filesOR;
	}
	
	//functia care returneaza lista cu reuniunea de fisiere - varianta supraincarcata
	public ArrayList<String> listOfOR(ArrayList<String> list1, String word2)
	{
		ArrayList<String> filesOR = new ArrayList<String>();
		//punem restul de fisiere care nu au fost adaugate in lista
		for(String s2 : fileList.get(word2))
		{
			//daca s2 nu se gaseste in lista atunci se adauga
			if(list1.contains(s2) == false)
			{
				filesOR.add(s2);
			}
		}
		return filesOR;
	}
	
	//functia care returneaza diferenta de fisiere
	public ArrayList<String> listOfNOT(String word1, String word2)
	{
		ArrayList<String> filesNOT = new ArrayList<String>();
		
		//parcurgem lista de fisiere a cuvantului 1
		for(String s1 : fileList.get(word1))
		{
			filesNOT.add(s1);
		}
		
		//parcurgem lista de fisiere a cuvantului 2 si daca gasim fisiere comune cu cele ale lui s1 le stergem
		for(String s2 : fileList.get(word2))
		{
			if(filesNOT.contains(s2) == true)
			{
				filesNOT.remove(s2);
			}
		}
		
		return filesNOT;
	}
	
	//functia care returneaza diferenta de fisiere - varianta supraincarcata
	public ArrayList<String> listOfNOT(ArrayList<String> list1, String word2)
	{
		ArrayList<String> filesNOT = new ArrayList<String>();
		filesNOT = list1;
		
		//parcurgem lista de fisiere a cuvantului 2 si daca gasim fisiere din list1 le stergem
		for(String s1 : fileList.get(word2))
		{
			if(filesNOT.contains(s1) == true)
			{
				filesNOT.remove(s1);
			}
		}
		return filesNOT;
	}
	
}
