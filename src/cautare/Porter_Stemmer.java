package cautare;

public class Porter_Stemmer {
	
	public Porter_Stemmer(){}
	 
      //functia care verifica daca un cuvant are sufixul dat
	  public boolean hasSuffix( String word, String suffix) 
	  {
		  if(word.endsWith(suffix))
		  {
			  return true;
		  }
		  else
		  {
			  return false;
		  }
	  }

	  //functia care verifica daca un caracter este vocala (plus cazul y)
	  public boolean vowel(char ch, char prev) 
	  {
	     switch ( ch ) {
	        case 'a': case 'e': case 'i': case 'o': case 'u': 
	          return true;
	        case 'y': {

	          switch ( prev ) {
	            case 'a': case 'e': case 'i': case 'o': case 'u': 
	              return false;

	            default: 
	              return true;
	          }
	        }
	        
	        default : 
	          return false;
	     }
	  }

	  //functia care il calculeaza pe m
	  public int measure(String stem)
	  {
		    int i=0, m = 0;
		    int length = stem.length();
	
		    while (i < length) 
		    {
		       //se parcurge cuvantul pana gasim o vocala
		       for (; i < length ; i++) 
		       {
		           if(i > 0) 
		           {
		              if(vowel(stem.charAt(i),stem.charAt(i-1)))
		                 break;
		           }
		           else 
		           {  
		              if (vowel(stem.charAt(i),'a'))
		                break; 
		           }
		       }
	
		       //se parcurge cuvantul pana gasim o consoana
		       for (i++ ; i < length ; i++)
		       {
		           if (i > 0) 
		           {
		              if (!vowel(stem.charAt(i),stem.charAt(i-1)))
		                  break;
		           }
		           else 
		           {  
		              if (!vowel(stem.charAt(i),'?'))
		                 break;
		           }
		       } 
		      //dupa aceste for-uri avem un grup cv, deci putem sa il incrementam pe m 
		      if (i < length) 
		      {
		         m++;
		         i++;
		      }
	    }
	    
	    return m;
	  }

	  //functia care verifica daca un cuvant contine vocale (Regula *v*)
	  public boolean containsVowel(String word)
	  {
	     for (int i=0 ; i < word.length(); i++)
	     {
	    	 if ( i > 0 ) 
	    	 {
	            if ( vowel(word.charAt(i),word.charAt(i-1)) )
	               return true;
		     }
	         else 
	         { 
	        	//verificam si prima litera si daca este y inseamna ca este consoana (pentru ca nu este precedata de o vocala)
	            if (vowel(word.charAt(0),'a'))
	               return true;
	         }
	     }
	     return false;
	  }

	  //verifica daca un cuvant este terminat cu cvc, unde al doilea c nu este W, Y sau X (Regula *o)
	  public boolean cvc(String str) 
	  {
	     int length=str.length();

	     if(length < 3)
	        return false;
	    //severifica ultimile 2 litere
	     if( (!vowel(str.charAt(length-1),str.charAt(length-2)) )
	        && (str.charAt(length-1) != 'w') && (str.charAt(length-1) != 'x') && (str.charAt(length-1) != 'y')
	        && (vowel(str.charAt(length-2),str.charAt(length-3))) ) 
	     {
	    	//se verifica si a treia litera de la coada si (caz separat pentru ca cuvantul ar putea avea doar 3 litere)
	        if(length == 3) 
	        {
	           if (!vowel(str.charAt(0),'?')) 
	              return true;
	           else
	              return false;
	        }
	        else 
	        {
	           if (!vowel(str.charAt(length-3),str.charAt(length-4)) ) 
	              return true; 
	           else
	              return false;
	        } 
	     }   
	  
	     return false;
	  }

	  //Pasul 1
	  public String step1(String str) 
	  {
		  //Pasul 1.a
		 //daca cuvantul se termina cu s
	     if (str.charAt(str.length()-1) == 's') 
	     {
	    	 //daca cuvantul se termina cu sses sau ies
	        if ((hasSuffix( str, "sses")) || (hasSuffix( str, "ies")))
	        {
	           //se sterge es in oricare din cele 2 cazuri
	           str = str.substring(0, str.length()-2);
	        }
	        else 
	        {
	           //daca cuvantul se termina cu un singur s, nu cu ss
	           if ( str.charAt( str.length()-2 ) != 's' ) 
	           {
	        	   //daca se intampla acest caz inseamna ca cuvantul contine in coada un singur s care trebuie sters
	        	   str = str.substring(0, str.length()-1);
	           }
	        }  
	     }
	     
	     //Pasul 1.b
	     //cazul in care cuvantul se termina cu eed  si m>0 => ee
	     if (hasSuffix(str,"eed"))
	     {
	           if (measure(str) > 0) 
	           {
	        	   str = str.substring(0, str.length()-1);
	           }
	     }
	     else 
	     {  
	        if ((hasSuffix( str,"ed")) || (hasSuffix( str,"ing"))) 
	        { 
	              if(hasSuffix( str,"ed") && containsVowel(str.substring(0,str.length()-2)))
	              {
	            	  str = str.substring(0,str.length()-2);
	              }
	              else if(hasSuffix( str,"ing") && containsVowel(str.substring(0,str.length()-3)))
	              {
	            	  str = str.substring(0, str.length()-3);
	              }

	              if (( hasSuffix( str,"at") ) || ( hasSuffix( str,"bl") ) || ( hasSuffix( str,"iz") )) 
	              {
	                 str += "e";
	              }
	              else 
	              {   
	                 int length = str.length(); 
	                 if ((str.charAt(length-1) == str.charAt(length-2)) //ultimile 2 litere sunt egale
	                    && (!vowel(str.charAt(length-1), str.charAt(length-2))) //si sunt consoane si nu sunt l sau z sau s
	                    && (str.charAt(length-1) != 'l') && (str.charAt(length-1) != 's') && (str.charAt(length-1) != 'z') ) 
	                 {    
	                    str = str.substring(0,str.length()-1);
	                 }
	                 else
	                 {
	                	 if ((measure( str ) == 1) && cvc(str)) 
		                    {
		                     	str += "e";
		                    }
	                 }   
	              }
	        }
	     }
	     
	     //pasul 1.c
	     //daca cuvantul contine vocale si ultima litera este y atunci y -> i
	     if (hasSuffix(str,"y") && containsVowel(str.substring(0,str.length()-1)))
	     {
	           str = str.substring(0, str.length()-1);
	           str += "i";
	     }
	        
	     return str;  
	  }
	  
	  //Pasul 2 - transformam sufixele curente in cele date
	  public String step2(String str) 
	  {
	     String[][] suffixes = { { "ational", "ate" },
                                { "tional",  "tion" },
                                { "enci",    "ence" },
                                { "anci",    "ance" },
                                { "izer",    "ize" },
                                { "iser",    "ize" },
                                { "abli",    "able" },
                                { "alli",    "al" },
                                { "entli",   "ent" },
                                { "eli",     "e" },
                                { "ousli",   "ous" },
                                { "ization", "ize" },
                                { "isation", "ize" },
                                { "ation",   "ate" },
                                { "ator",    "ate" },
                                { "alism",   "al" },
                                { "iveness", "ive" },
                                { "fulness", "ful" },
                                { "ousness", "ous" },
                                { "aliti",   "al" },
                                { "iviti",   "ive" },
                                { "biliti",  "ble" }};
	     
	     for ( int index = 0 ; index < suffixes.length; index++ ) {
	         if (hasSuffix (str, suffixes[index][0])) 
	         {
	            if (measure(str) > 0) 
	            {
	               str = str.substring(0, str.length() - suffixes[index][0].length());
	               str += suffixes[index][1];
	               return str;
	            }
	         }
	     }

	     return str;
	  }

	  //Pasull 3 - transformam sufixele curente in cele date
	  public String step3(String str) 
	  {
	        String[][] suffixes = { { "icate", "ic" },
                                   { "ative", "" },
                                   { "alize", "al" },
                                   { "alise", "al" },
                                   { "iciti", "ic" },
                                   { "ical",  "ic" },
                                   { "ful",   "" },
                                   { "ness",  "" }};

	        for ( int index = 0 ; index<suffixes.length; index++ ) {
	            if (hasSuffix ( str, suffixes[index][0]))
	            {
	            	if (measure (str) > 0) 
	            	{
	            		str = str.substring(0, str.length() - suffixes[index][0].length());
	            		str += suffixes[index][1];
		                return str;
		               }
	            }
	        }
	        return str;
	  }

	  //Pasul 4 - transformam sufixele curente in cele date
	  public String step4(String str) 
	  {
	        
	     String[] suffixes = { "al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement", "ment", "ent", "sion", "tion",
	                           "ou", "ism", "ate", "iti", "ous", "ive", "ize", "ise"};
	        
	     for ( int index = 0 ; index<suffixes.length; index++ ) 
	     {
	         if (hasSuffix (str, suffixes[index])) 
	         {      
	            if (measure (str) > 1) 
	            {
	               str = str.substring(0, str.length() - suffixes[index].length());
	               return str;
	            }
	         }
	     }
	     return str;
	  }

	  //Pasul 5 - mici modificari
	  public String step5(String str) 
	  {
		 //Pasul 5.a
		 //(m>1) E
	     if (str.charAt(str.length()-1) == 'e') 
	     { 
	        if (measure(str) > 1) 
	        {
	           str = str.substring(0, str.length()-1);
	        }
	        else
	        {
	        	//(m=1 and not *o) E
	        	if ( measure(str) == 1 ) 
	        	{
		              if (!cvc(str))
		              {
		            	  str = str.substring(0, str.length()-1);
		              }
		           }
	        }
	     }
	     //Pasul 5.b
	     if ( str.length() == 1 )
	        return str;
	     if ( (str.charAt(str.length()-1) == 'l') && (str.charAt(str.length()-2) == 'l') && (measure(str) > 1) )
	     {
	    	str = str.substring(0, str.length()-1); 
	     } 
	     return str;
	  }

	  //trecem cuvantul prin toti pasii
	  public String stripSuffixes( String str ) 
	  {
		  String word_1="", word_2="", word_3="", word_4="", word_5="";
		  
		  //System.out.println("Before step 1: " + str);
		  word_1 = step1(str);
		  //System.out.println("After step 1: " + word_1);
		  
	     if (word_1.length() >= 1)
	     {
	    	 //System.out.println("Before step 2: " + word_1);
	    	 word_2 = step2(word_1);
	    	 //System.out.println("After step 2: " + word_2);
	     }
	     if (word_2.length() >= 1)
	     {
	    	 //System.out.println("Before step 3: " + word_2);
	    	 word_3 = step3(word_2);
	    	 //System.out.println("After step 3: " + word_3);
	     }
	     if (word_3.length() >= 1)
	     {
	    	 //System.out.println("Before step 4: " + word_3);
	    	 word_4 = step4(word_3);
	    	 //System.out.println("After step 4: " + word_4);
	     }
	     if (word_4.length() >= 1)
	     {
	    	 //System.out.println("Before step 5: " + word_4);
	    	 word_5 = step5(word_4);
	    	 //System.out.println("After step 5: " + word_5);
	     }

	     return word_5; 
	  }


	  //aplicam algoritmul complet
	  public String PorterAlgoritm(String str)
	  {
	    str = str.toLowerCase();
	  
	    if (( str != "" ) && (str.length() > 2)) 
	    {
	          str = stripSuffixes(str);
	    }   

	    return str;
	 }
}
