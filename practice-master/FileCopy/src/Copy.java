import java.io.*;


public class Copy {
	
	public static void main  (String [] arg) throws IOException{
		
		File Dir=new File ("F:\\JavaFiles\\");
		Dir.mkdir();		
		String [] listt=Dir.list();
		int Counter=0;	
		for(String data:listt)
		{		
		
			String [] oldname= data.split("-", 2);
			String dir=oldname[0].trim();String filename=oldname[1].trim();
		    dir=dir.replace(",","");		    
			File MyDir=new File ("F:\\JavaFiles\\"+dir);
			if(!MyDir.exists())
			MyDir.mkdir();
			
			InputStream inStream = null;
			OutputStream outStream = null;		 
		    	try{
		 
		    	    File infile =new File("F:\\JavaFiles\\"+data);		    	       	
		    	    File outfile =new File("F:\\JavaFiles\\"+dir+"\\"+filename);
		 
		    	    inStream = new FileInputStream(infile);
		    	    outStream = new FileOutputStream(outfile);
		 
		    	    byte[] buffer = new byte[10240];		 
		    	    int length;
		    	   
		    	    while ((length = inStream.read(buffer)) > 0)	 
		    	    	outStream.write(buffer, 0, length);
		    	    
		    	    inStream.close();
		    	    outStream.close();		 
		    	    infile.delete();
		    	    Counter++;
		    	}catch(IOException e){
		    	    e.printStackTrace();
		    	}				
			}System.out.println(Counter+ " files copied successfully. Thanks !");		
	}
}
