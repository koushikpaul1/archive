import java.io.*;


public class NameList {
	
	public static void main (String [] arg) throws IOException{
		String directory="F:\\Koushik'sKindleCollection\\";
		File Dir=new File (directory);		
		String [] listt=Dir.list();
		int authorCounter=0; int bookCounter=0;
		File List=new File("Filelist.txt");
		List.createNewFile(); 
			PrintWriter pw=new PrintWriter(List);
		for(String fn : listt){
			authorCounter++;int books=0;
			File SubDir=new File (directory+fn);
			String [] filelist=SubDir.list();
			String DirFile =null;String fn2 ="";//=authorCounter+"("+books+")"+"	"+fn +"	" ;
			if(filelist!=null)
			for(String fn1 : filelist){
				bookCounter++;books++;
				DirFile = DirFile+fn1+",";
				fn2=fn2+fn1+",";
			}
			DirFile =authorCounter+"	"+books+"	"+fn +"	"+fn2 ;
			pw.println(DirFile);
			System.out.println( DirFile);}
			pw.println("There are  "+bookCounter+ "  books writen by " +authorCounter+ " different authors.");
			pw.close();
			System.out.println("\n\n\nThere are  "+bookCounter+ "  books writen by " +authorCounter+ " different authors.");
		
	}
}
