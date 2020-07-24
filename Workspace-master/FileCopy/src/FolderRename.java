import java.io.*;

public class FolderRename {

	public static void main(String[] arg) throws IOException {
		String directory = "F:\\Koushik'sKindleCollection\\";
		String trash = "F:\\trash\\";
		File Dir = new  File(directory);
		String[] AuthorList = Dir.list();
		for (String al : AuthorList) {
			File SubDir = new File(directory + al);
			String[] booklist = SubDir.list();
			if (booklist != null)
				for (String bl : booklist) {
					String oldir = directory + al + "\\" + bl;
					File olddir = new File(oldir);
					int len = bl.length();
					if (bl.charAt(len - 1) == ')') {
						//System.out.println(directory + al + "\\" + bl);
						int position = bl.lastIndexOf('(');
						String bl1 = bl.substring(0, position);
						String newdr = trash  + bl1;
						File newdir = new File(newdr);
						System.out.println(olddir.renameTo(newdir));
						System.out.println(directory + al + "\\" + bl);
					}
				}
		}
		System.out.println("success");
	}

}
