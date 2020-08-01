package cautare;

public class Test_Class {

	public static void main(String[] args) {
		
		/*
		//index direct Mongo
		Index_Direct_Mongo idm = new Index_Direct_Mongo();
		idm.CrossingDirectors(new File("Struct_dir"));
		idm.printCollection();
		
		//index invers Mongo
		Index_Indirect_Mongo iim = new Index_Indirect_Mongo();
		iim.ProcessingFilesReverse();
		iim.printCollection();
		*/
		
		//cautarea booleana mongo
		//Cautare_Booleana_Mongo cb = new Cautare_Booleana_Mongo();
		//cb.processSearchWords("growth - risk ~ green");
		//cb.printFinalList();
		
		//cautare vectoriala mongo
		Cautare_Vectoriala_Mongo cvm = new Cautare_Vectoriala_Mongo();
		cvm.processSearchWords("will - provid + car ~ ground");
		cvm.printFinalList();
	}
}
