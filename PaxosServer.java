import java.lang.ProcessBuilder;
import java.lang.Process;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.charset.CharsetDecoder;  
import java.nio.charset.CharsetEncoder;  
import java.nio.charset.Charset;  
import java.nio.CharBuffer;  
import java.io.IOException;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.net.ConnectException;

public class PaxosServer 
{
	private static final int portBase = 2139;
	private static final int MaxClientNum = 100;
	private static final int cmdLength = 30;
	private static final int MaxWaitingRound = 2;
	private static final long MaxWaitingReproposeTime = 300;
	private static final long MaxWaitingSelectTime = 100;
	private static final double GeneralLostRate = -1;
	private static final double PrepareLostRate = GeneralLostRate;
	private static final double RePrepareLostRate = GeneralLostRate;
	private static final double AcceptLostRate = GeneralLostRate;
	private static final double ReAcceptLostRate = GeneralLostRate;
	private static final double ChosenLostRate = GeneralLostRate;
	private static final double AskLostRate = GeneralLostRate;
	private static final double AnswerLostRate = GeneralLostRate;

	private static int serverID;
	private static int numClient = 0;
	private static int numServer;
	private static int numMajority;
	private static boolean distinLearner;
	private static boolean proposed;
	private static int cntPropNum = 0;
	private static int cntInsID = 0;
	private static int highestInsID = 0;
	private static long prevRoundStartTime;

	private static ExtendedHashMap<Integer, String> highestRePrepareValue = new ExtendedHashMap<Integer, String>("");
	private static ExtendedHashMap<Integer, String> highestAcceptedValue = new ExtendedHashMap<Integer, String>("");
	private static ExtendedHashMap<Integer, Integer> numAccepted = new ExtendedHashMap<Integer, Integer>(0);
	private static ExtendedHashMap<Integer, Integer> numPrepareResponse = new ExtendedHashMap<Integer, Integer>(0);
	private static ExtendedHashMap<Integer, Integer> highestAcceptedPropNum = new ExtendedHashMap<Integer, Integer>(-1);
	private static ExtendedHashMap<Integer, Integer> highestRespondedPropNum = new ExtendedHashMap<Integer, Integer>(-1);
	private static ExtendedHashMap<Integer, Integer> highestRePrepareNum = new ExtendedHashMap<Integer, Integer>(-1);

	private static StateMachine stateMachine = new StateMachine();
	private static Selector selector;
	private static LinkedList<ClientCommand> clientRequestQueue = new LinkedList<ClientCommand>();
	private static ArrayList<LinkedList<String> > writeQueue = new ArrayList<LinkedList<String> >();
	private static ArrayList<SelectionKey> selKeyArray = new ArrayList<SelectionKey>();

	private static void newRoundInit()
	{
		// currently only calls when "chosen". what if the chosen message is lost (not one get it, e.g. distinguished learner died before sending msg)? need a timeout to restart?
		// if some learners get it, they will start a new round. then this one will learn it by asking eventually
		System.out.println("newRoundInit");

		prevRoundStartTime = System.currentTimeMillis();
		distinLearner = false;
		cntPropNum += (new Random()).nextInt(10) + 1;
		// my definition of highestInsID: highest ins id that I know that indeed has a chosen value (via "chosen" or "answer")
		// so if each server only uses this value + 1 as the new ins id, there won't be an empty instance id with no value chosen
		cntInsID = highestInsID + 1;

		proposed = false;
		tryPropose();

		if (cntInsID - stateMachine.nextProcessInsID >= MaxWaitingRound)
		{
			System.out.println("asking from " + stateMachine.nextProcessInsID + " to " + (highestInsID-1));
			for (int i = stateMachine.nextProcessInsID; i < highestInsID; ++i)
				if (stateMachine.getConsensus(i).equals("none"))
					for (int j = 1; j < numServer; ++j)
						addIntoWriteQueue(j, extendCommand(cntInsID, "ask " + i));
		}
	}

	public static String getCntRequest()
	{
		return clientRequestQueue.get(0).command;
	}

	public static int getCntClientIndx()
	{
		return (Integer)clientRequestQueue.get(0).key.attachment();
	}

	public static void tryPropose()
	{
		System.out.println("tryPropose");
		if (clientRequestQueue.size() > 0)
		{
			System.out.println("has sth, going to propose");
			proposed = true;
			for (int i = 1; i <= numServer; ++i)
				addIntoWriteQueue(i, extendCommand(cntInsID, "prepare " + cntPropNum + " " + getCntRequest()));
		}
	}

	public static void newClientRequest(ClientCommand cmd)
	{
		System.out.println("new client request");
		cmd.key.interestOps(cmd.key.interestOps() ^ SelectionKey.OP_READ);
		clientRequestQueue.add(cmd);
		if (!proposed)
			tryPropose();
	}

	public static SocketChannel createSocketChannel(String hostName, int port) 
	{
		try
		{

    		SocketChannel sChannel = SocketChannel.open(new InetSocketAddress(hostName, port));
    		sChannel.configureBlocking(false);
		return sChannel;
		
		}
		catch (Exception e)
		{
		}
		return null;
	}

	public static ServerSocketChannel createServerSocketChannel(int port) 
	{
		try
		{

    		ServerSocketChannel ssChannel = ServerSocketChannel.open();
    		ssChannel.configureBlocking(false);
    		ssChannel.socket().bind(new InetSocketAddress(port));
		System.out.println("server socket bind " + port);
		return ssChannel;

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public static void registerOtherServers(ServerSocketChannel listenChannel) throws Exception
	{
		// 1 - numServer: accept other machine's connection as a server
		// numServer+1 - 2*numServer: connect to another machine as a client
		// 2 paths for each pair of servers actually, anyone will work
		// 2*numServer+1 - __ : clients

		SocketChannel[] toServer = new SocketChannel[numServer];
		int count1 = 0;
		int count2 = 0;
		while (true)
		{
			SocketChannel tmp;
			for (int i = 0; i < numServer; ++i)
			{
				if (toServer[i] == null)
					try
					{
						toServer[i] = createSocketChannel("127.0.0.1", portBase + i);
						if (toServer[i] != null)
						{
							System.out.println(toServer[i].socket());
							++count1;
						}
					}
					catch (Exception e)
					{
					}
			}
			if ((tmp = listenChannel.accept()) != null)
			{
				++count2;
				tmp.configureBlocking(false);
				addSelKey(tmp.register(selector, SelectionKey.OP_READ), count2);
				System.out.println(serverID + " " + tmp.socket());
			}
			if (count1 == numServer && count2 == numServer)
				break;
		}
		for (int i = 0; i < numServer; ++i)
			addSelKey(toServer[i].register(selector, SelectionKey.OP_READ), numServer + i + 1);
	}

	private static void removeClientConnection(SelectionKey key)
	{
		try
		{

		int indx = (Integer)(key.attachment());
		if (indx == -1)
			return;
		int replaceIndx = 2*numServer + numClient;
		System.out.println("remove client connection, replacing " + replaceIndx + " " + indx);

		selKeyArray.set(indx, selKeyArray.get(replaceIndx));
		writeQueue.set(indx, writeQueue.get(replaceIndx));
		selKeyArray.get(indx).attach(indx);
		key.attach(-1);
		((SocketChannel)key.channel()).close();
		key.cancel();
		numClient--;

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private static String readFromClientSocketChannel(SelectionKey key)
	{
		try
		{

		SocketChannel channel = (SocketChannel)key.channel();
		ByteBuffer single = ByteBuffer.allocateDirect(1);
		ByteBuffer buffer = ByteBuffer.allocateDirect(cmdLength);
		single.clear();
		buffer.clear();

		int hasRead = 0;
		while (true)
		{
			int justRead = channel.read(single);
			if (justRead == -1) 
			{
				System.out.println("client connection closed, read");
				removeClientConnection(key);
				return "";
			}
			if (justRead > 0)
			{
				hasRead += justRead;
				buffer.put(single.get(0));
				if (single.get(0) == (byte)(10))
					break;
			}
			if (hasRead >= cmdLength)
			{
				System.out.println("client command too long!");
				break;
			}
			single.clear();
		}

		buffer.position(0);
        	Charset charset = Charset.defaultCharset();  
        	CharsetDecoder decoder = charset.newDecoder();  
        	String s = decoder.decode(buffer).toString().trim();
		return s;

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	private static String readFromSocketChannel(SelectionKey key) 
	{
		try
		{

		SocketChannel channel = (SocketChannel)key.channel();
		ByteBuffer buffer = ByteBuffer.allocateDirect(cmdLength);
		buffer.clear();

		int hasRead = 0;
		while (hasRead < cmdLength)
		{
			int justRead = channel.read(buffer);
			if (justRead == -1) 
			{
				System.out.println("server connection closed, read");
				return "";
			}
			hasRead += justRead;
		}

		buffer.position(0);
        	Charset charset = Charset.defaultCharset();  
        	CharsetDecoder decoder = charset.newDecoder();  
        	String s = decoder.decode(buffer).toString().trim();
		return s;

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	private static void writeToSocketChannel(SelectionKey key, String cmd)
	{
		if (!key.isValid())
			return;
  		try
		{

		SocketChannel channel = (SocketChannel)key.channel();
        	Charset charset = Charset.defaultCharset();  
        	CharsetEncoder encoder = charset.newEncoder();  
    		ByteBuffer buffer = encoder.encode(CharBuffer.wrap(cmd));
		buffer.position(0);
		
		int hasWritten = 0;
		while (hasWritten < cmd.length())
		{
			int justWrote = channel.write(buffer);
			hasWritten += justWrote;
		}

		}
		catch (Exception e)
		{
			System.out.println("connection closed, write");
			if ((Integer)(key.attachment()) > 2*numServer)
				removeClientConnection(key);
			//e.printStackTrace();
		}
	}

	public static void addIntoWriteQueue(int indx, String command)
	{
		if (indx == -1)
			return;
		SelectionKey tmpkey = selKeyArray.get(indx);
		if (tmpkey.isValid())
		{
			System.out.println("addintowritequeue " + indx + " " + command);
			writeQueue.get(indx).add(command);
			int tmp = tmpkey.interestOps() | SelectionKey.OP_WRITE; 
			tmpkey.interestOps(tmp);
		}
	}
	
	public static String popFromWriteQueue(int indx)
	{
		SelectionKey tmpkey = selKeyArray.get(indx);
		if (tmpkey.isValid() && writeQueue.get(indx).size() == 1)
		{
			int tmp = tmpkey.interestOps() ^ SelectionKey.OP_WRITE; 
			tmpkey.interestOps(tmp);
		}
		String command = writeQueue.get(indx).remove();
		System.out.println("popfromwritequeue " + indx + " " + command);
		return command;
	}

	public static void addSelKey(SelectionKey key, int x)
	{
		key.attach(x);
		if (x >= selKeyArray.size())
			selKeyArray.add(key);
		else
			selKeyArray.set(x, key);
	}

	public static void main(String[] args) 
	{
		serverID = Integer.parseInt(args[0]);
		numServer = Integer.parseInt(args[1]);
		numMajority = numServer / 2 + 1;
		System.out.println("Server No." + serverID + " launched.");
		for (int i = 0; i < 2*numServer + MaxClientNum + 1; ++i)
			writeQueue.add(new LinkedList<String>());

		try 
		{

		selector = Selector.open();
		ServerSocketChannel listenChannel = createServerSocketChannel(2139 + serverID);
		addSelKey(listenChannel.register(selector, SelectionKey.OP_ACCEPT), 0);

		registerOtherServers(listenChannel);
		System.out.println("registration done");
		newRoundInit();

		while (true) 
		{
			try
		    	{
				selector.select(MaxWaitingSelectTime);
		    	} 
			catch (IOException e) 
		    	{
				e.printStackTrace();
				break;
		    	}
			Iterator<SelectionKey> keyIter = selector.selectedKeys().iterator();
			while (keyIter.hasNext())
			{
				SelectionKey selKey = keyIter.next();
				if (selKey.readyOps() > 0)
			 		processSelectionKey(selKey);
				keyIter.remove();
		    	}
			System.out.println("------ one selection --------");
			//Thread.sleep(7000);
			long cntTime = System.currentTimeMillis();
			if (cntTime - prevRoundStartTime > MaxWaitingReproposeTime)
				newRoundInit();
		}
		
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

	public static void processSelectionKey(SelectionKey selKey) throws IOException 
	{
		int indx = (Integer)selKey.attachment();
		System.out.println("processing key indx = " + indx);
	    	if (selKey.isValid() && selKey.isAcceptable() && numClient < MaxClientNum)  // temp
	    	{
			System.out.println("acceptable");
			SocketChannel newConn = ((ServerSocketChannel)selKey.channel()).accept();
			System.out.println("new client connection: " + newConn.socket().getInetAddress() + " " + newConn.socket().getPort());
			newConn.configureBlocking(false); 
			addSelKey(newConn.register(selector, SelectionKey.OP_READ), (++numClient) + 2*numServer);
	    	}
	    	if (selKey.isValid() && selKey.isReadable()) 
		{
			String command;
			if (indx <= 2*numServer)
				command = readFromSocketChannel(selKey);
			else
				command = readFromClientSocketChannel(selKey);
			System.out.println("readable " + command);

			if (indx <= 2*numServer) // read from another server
			{
				int flyingInsID = Integer.parseInt(getField(command, -1));
				System.out.println("flying instance ID = " + flyingInsID);
				if (flyingInsID != cntInsID)
				{
					System.out.println("     Some One is Falling Behind!"); 
					// do something?
				}
				if (command.startsWith("prepare"))
				{
					if (Math.random() < PrepareLostRate)
						return;
					int propNum = Integer.parseInt(getField(command, 1));
					if (propNum <= highestRespondedPropNum.getInt(flyingInsID))
						return;
					highestRespondedPropNum.put(flyingInsID, propNum);
					addIntoWriteQueue(indx, extendCommand(flyingInsID, "re-prepare " + highestAcceptedPropNum.getInt(flyingInsID) + " " + highestAcceptedValue.getStr(flyingInsID)));
				}
				else if (command.startsWith("re-prepare"))
				{
					if (Math.random() < RePrepareLostRate)
						return;
					numPrepareResponse.put(flyingInsID, numPrepareResponse.getInt(flyingInsID) + 1);
					int propNum = Integer.parseInt(getField(command, 1));
					if (propNum > highestRePrepareNum.getInt(flyingInsID))
					{
						highestRePrepareNum.put(flyingInsID, propNum);
						highestRePrepareValue.put(flyingInsID, getField(command, 2)); 
					}
					if (numPrepareResponse.getInt(flyingInsID) == numMajority)
					{
						String tmp;
						if (highestRePrepareNum.getInt(flyingInsID) == -1)
							tmp = extendCommand(flyingInsID, "accept " + cntPropNum + " " + getCntRequest());
						else
							tmp = extendCommand(flyingInsID, "accept " + cntPropNum + " " + highestRePrepareValue.get(flyingInsID));
						for (int i = 1; i <= numServer; ++i)
							addIntoWriteQueue(i, tmp);
					}
				}
				else if (command.startsWith("accept"))
				{
					if (Math.random() < AcceptLostRate)
						return;
					int propNum = Integer.parseInt(getField(command, 1));
					if (propNum < highestRespondedPropNum.getInt(flyingInsID))
						addIntoWriteQueue(indx, extendCommand(flyingInsID, "re-accept rej"));
					else
					{
						highestAcceptedPropNum.put(flyingInsID, propNum);
						highestAcceptedValue.put(flyingInsID, getField(command, 2));
						addIntoWriteQueue(indx, extendCommand(flyingInsID, "re-accept accept"));
					}
				}
				else if (command.startsWith("re-accept"))
				{
					if (Math.random() < ReAcceptLostRate)
						return;
					if (command.startsWith("re-accept rej"))
					{
					}
					else
					{
						numAccepted.put(flyingInsID, numAccepted.getInt(flyingInsID) + 1);
						if (numAccepted.getInt(flyingInsID) == numMajority)
						{
							for (int i = 1; i <= numServer; ++i)
								addIntoWriteQueue(i, extendCommand(flyingInsID, "chosen " + getCntRequest()));
							distinLearner = true;
						}
					}
				}
				else if (command.startsWith("chosen"))
				{
					if (Math.random() < ChosenLostRate)
						return;
					String value = getField(command, 1);
					stateMachine.input(flyingInsID, value);
					if (distinLearner)
					{
						addIntoWriteQueue(getCntClientIndx(), stateMachine.getOutput());
						clientRequestQueue.remove();
					}
					if (flyingInsID > highestInsID)
						highestInsID = flyingInsID;
					newRoundInit(); 
				}
				else if (command.startsWith("ask")) // when to send it?  maybe a timeout?
				{
					if (Math.random() < AskLostRate)
						return;
					int askingInsID = Integer.parseInt(getField(command, 1));
					addIntoWriteQueue(indx, extendCommand(askingInsID, "answer " + stateMachine.getConsensus(askingInsID)));
				}
				else if (command.startsWith("answer"))
				{
					if (Math.random() < AnswerLostRate)
						return;
					String answer = getField(command, 1);
					int askingInsID = Integer.parseInt(getField(command, -1));
					if (!answer.equals("none"))
					{
						stateMachine.input(askingInsID, answer);
						if (askingInsID > highestInsID) 
							highestInsID = askingInsID; 
					} 
				}
			}
			else // read from a real client, lock & unlock
			{
				if (command.length() > 0)
					newClientRequest(new ClientCommand(command, selKey));
			}
	    	}
	    	if (selKey.isValid() && selKey.isWritable()) 
		{
			System.out.println("writeable");
			SocketChannel sChannel = (SocketChannel)selKey.channel();
			writeToSocketChannel(selKey, popFromWriteQueue(indx));

			if (indx <= 2 * numServer) // write to another server
			{
			}
			else if (selKey.isValid()) // write to a real client 
			{
				//System.out.println("care about READ again");
				selKey.interestOps(selKey.interestOps() | SelectionKey.OP_READ);
			}
	    	}
	}

	public static String extendCommand(int insID, String s)
	{
		s = s + " " + insID;
		int len = s.length();
		for (int i = 0; i < cmdLength - len; ++i)
			s = s + "#";
		//System.out.println("extend command " + s);
		return s;
	}

	public static String getField(String s, int indx)
	{
		s = s.substring(0, s.indexOf('#'));
		String[] splitted = s.split(" ");
		if (indx == -1)
			return splitted[splitted.length-1];
		return splitted[indx];
	}

	private static class ClientCommand
	{
		private String command;
		private SelectionKey key; 

		ClientCommand(String command, SelectionKey key)
		{
			this.command = command;
			this.key = key;
		}
	}
}
