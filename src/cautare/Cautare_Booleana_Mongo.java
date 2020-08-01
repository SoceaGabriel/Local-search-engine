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
import com.mongodb.client.MongoDatabase;

public class Cautare_Booleana_Mongo {

	private ArrayList<String> keywords;
	private ArrayList<String> operators;
	private HashMap<String, ArrayList<String>> fileList;
	
	//obiecte necesare in lucru cu baza de date
	MongoClient mongoClient;
	MongoDatabase database;
	MongoCollection<Document> collection = null;
	
	//constructorul fara parametri
	public Cautare_Booleana_Mongo()
	{
		//instantiem listele folosite in cautare
		keywords = new ArrayList<String>();
		operators = new ArrayList<String>();
		fileList = new HashMap<String, ArrayList<String>>();
		
		//instantiem obiectele necesare lucrului cu bd
		//creem conexiunea cu mongo
		mongoClient = new MongoClient ("localhost", 27017);
		
		//aducem baza de date intr-un obiect java
		database = mongoClient.getDatabase("Indexare");
		
		//aducem colectia care contine indexul direct intr-un obiect java
		collection = database.getCollection("IndexInvers");
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
            System.out.println("Cautarea booleana: Nu s-a putut deschide fisierul de intrare!");
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
			if((arrOfStr[i].contains("+") ||  arrOfStr[i].contains("-") || arrOfStr[i].contains("~")) && !isStopWordBR(arrOfStr[i+1])){
				
				operators.add(arrOfStr[i]);
				
			}else if(isExceptionBR(arrOfStr[i])==true){
				
				keywords.add(arrOfStr[i]);
				
			}else if(isStopWordBR(arrOfStr[i])){
				
				continue;
				
			}
			else{
				
				keywords.add(arrOfStr[i]);
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
								//System.out.println(fileSplit.get("File"+j));
								listOfFiles.add((String) fileSplit.get("File"+j));
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
						//ArrayList<String> ar = new ArrayList<String>();
						//ar = listOfAND(auxList, keywords.get(i));
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
		ArrayList<String> finalList = new ArrayList<String>();
		
		//aplicam toate operatiile
		finalList = applyOperators();
		if(finalList.isEmpty())
		{
			System.out.println("Nu s-au gasit pagini care sa indeplineasca criteriile cerute!");
		}
		else
		{
			System.out.println("Lista de documente care satisface cautarea utilizatorului: ");
			for(int i=0; i<finalList.size(); i++)
			{
				System.out.println(finalList.get(i));
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
