import java.lang.ProcessBuilder;
import java.lang.Process;
import java.io.*;

public class LockService
{

	private static int numServer;

	public static void main(String[] args) 
	{
		numServer = Integer.parseInt(args[0]);
		System.out.println("Lock Service Launched, numServer = " + numServer);

		try
		{
		BufferedReader[] br = new BufferedReader[numServer];
		for (int i = 0; i < numServer; ++i)
		{
			ProcessBuilder pb = new ProcessBuilder("java", "PaxosServer", i+"", numServer+""); 
			Process process = pb.start();
    			br[i] = new BufferedReader(new InputStreamReader(process.getInputStream()));
		}
		while (true)
		{
    			String line;
			for (int i = 0; i < numServer; ++i)
			{
				System.out.println("br " + i + " start");
    				while ((line = br[i].readLine()) != null) 
      					System.out.println(line);
				System.out.println("br " + i + " end");
			}
		}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
