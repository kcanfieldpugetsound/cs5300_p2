import java.util.Scanner;
import java.io.File;


public class FileReader{
	public static void main(String [] args){
		

		try{
			Scanner sc = new Scanner(new File("edges.txt"));
			String line = sc.nextLine();

			String[] stringArray = line.split("\\s+");

			for (int i = 0; i < stringArray.length; i++)
				System.out.println("String at " + i + ": " + stringArray[i]);
		}
		catch(Exception e){
			System.err.println("caught exception");
		}
		


	}
}