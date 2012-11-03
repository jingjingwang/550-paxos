import java.lang.ProcessBuilder;
import java.lang.Process;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.HashMap;
import java.util.HashSet;
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
	private static final int serverPortBase = 2139;
	private static final int clientPortBase = 2139;
	private static final int MaxClientNum = 100;
	private static final int MaxServerNum = 20;
	private static final int cmdLength = 50;
	private static final int MaxWaitingRound = 1;
	private static final long MaxWaitingSelectTime = 100;
	private static final double GeneralLostRate = 0.5;
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
	private static boolean proposed;
	private static int cntPropNum = 0;
	private static int cntInsID = 1;
	private static int highestInsID = 1;
	private static int cntNumClient = 0;
	private static int cntNumServer = 0;

	private static ExtendedHashMap<Integer, String> highestAcceptedValue = new ExtendedHashMap<Integer, String>("");
	private static ExtendedHashMap<Integer, String> highestRePrepareValue = new ExtendedHashMap<Integer, String>("");
	private static ExtendedHashMap<Integer, Integer> numAccepted = new ExtendedHashMap<Integer, Integer>(0);
	private static ExtendedHashMap<Integer, Integer> numPrepareResponse = new ExtendedHashMap<Integer, Integer>(0);
	private static ExtendedHashMap<Integer, Integer> highestAcceptedPropNum = new ExtendedHashMap<Integer, Integer>(-1);
	private static ExtendedHashMap<Integer, Integer> highestRespondedPropNum = new ExtendedHashMap<Integer, Integer>(-1);
	private static ExtendedHashMap<Integer, Integer> highestRePrepareNum = new ExtendedHashMap<Integer, Integer>(-1);
	//private static ExtendedHashMap<Integer, Integer> distinProposer = new ExtendedHashMap<Integer, Integer>(0);
	
	private static StateMachine stateMachine = new StateMachine();
	private static Selector selector;
	private static LinkedList<ClientCommand> clientRequestQueue = new LinkedList<ClientCommand>();
	private static HashSet<SelectionKey> connAsClient = new HashSet<SelectionKey>();

	private static LinkedList<PendingAnswer> pendingToAnswer = new LinkedList<PendingAnswer>();

	private static HashMap<SelectionKey, LinkedList<String> > writeQueue = new HashMap<SelectionKey, LinkedList<String> >();
	//private static ArrayList<SelectionKey> selKeyArray = new ArrayList<SelectionKey>();

	private static void newRoundInit()
	{
		// currently only calls when "chosen". what if the chosen message is lost (not one get it, e.g. distinguished learner died before sending msg)? need a timeout to restart?
		// if some learners get it, they will start a new round. then this one will learn it by asking eventually
		//System.out.println("newRoundInit");

		cntPropNum += (new Random()).nextInt(10) + 1;
		// my definition of highestInsID: highest ins id that I know that indeed has a chosen value (via "chosen" or "answer")
		// so if each server only uses this value + 1 as the new ins id, there won't be an empty instance id with no value chosen

		
		if (stateMachine.getInput(cntInsID).equals("none")) //cntInsID == highestInsID + 1)
		{
			// not a new round, update proposer's state
			// probably a good motivation to seperate 3 roles here
			numAccepted.put(cntInsID, 0);
			numPrepareResponse.put(cntInsID, 0);
			//distinProposer.put(cntInsID, 0);
		}
		else // has result for this instance already, could go to a new one
		{
			if (getCntRequest() != null)
			{
				//System.out.println("cnt request is chosen! " + getCntRequest());
				if (stateMachine.getInput(cntInsID).equals(getCntRequest())) // the value is successfully chosen, move to the next request
				{
					System.out.println("add pendig answer " + cntInsID + " " + clientRequestQueue.get(0).command);
					pendingToAnswer.add(new PendingAnswer(clientRequestQueue.get(0), cntInsID));
					clientRequestQueue.remove();
					checkPendingAnswer();
				}
			}
			cntInsID = highestInsID + 1;
		}

		proposed = false;
		tryPropose();
	}

	public static void checkPendingAnswer()
	{
		System.out.println("checkpendinganswer " + pendingToAnswer.size());
		while (pendingToAnswer.size() > 0)
		{
			int tmpID = pendingToAnswer.get(0).insID;
			String tmpAns = stateMachine.getOutput(tmpID);
			System.out.println(tmpID + "\t" + tmpAns);
			if (tmpAns != null)
			{
				addIntoWriteQueue(pendingToAnswer.get(0).clientCommand.key, tmpAns); 
				pendingToAnswer.remove();
			}
			else
				break;
		}
	}

	public static void broadcastToAllServers(String str)
	{
		Iterator<SelectionKey> iter = connAsClient.iterator();
		while (iter.hasNext())
		{
			addIntoWriteQueue(iter, str);
			iter = iter.next();
		}

	}

	public static void checkIfAskMissedInstance(int flyingInsID)
	{
		if (flyingInsID - stateMachine.nextProcessInsID >= MaxWaitingRound)
		{
			//System.out.println("asking from " + stateMachine.nextProcessInsID + " to " + (flyingInsID-1));
			for (int i = stateMachine.nextProcessInsID; i < flyingInsID; ++i)
				if (stateMachine.getInput(i).equals("none"))
					broadcastToAllServers(extendCommand(cntInsID, "ask " + i));
					//for (int j = 1; j <= numServer; ++j)
					//	addIntoWriteQueue(j, );
		}
	}

	public static String getCntRequest()
	{
		if (clientRequestQueue.size() == 0)
			return null;
		return clientRequestQueue.get(0).command;
	}

	public static int getCntClientIndx()
	{
		return (Integer)clientRequestQueue.get(0).key.attachment();
	}

	public static void tryPropose()
	{
		//System.out.println("tryPropose");
		if (clientRequestQueue.size() > 0)
		{
			//System.out.println("has sth, going to propose");
			proposed = true;
			broadcastToAllServers(extendCommand(cntInsID, "prepare " + cntPropNum + " " + getCntRequest()));
			//for (int i = 1; i <= numServer; ++i)
			//	addIntoWriteQueue(i, );
		}
	}

	public static void newClientRequest(ClientCommand cmd)
	{
		//System.out.println("new client request");
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
		//System.out.println("server socket bind " + port);
		return ssChannel;

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public static void connectToOtherServer() throws Exception
	{
		// 1 - numServer: accept other machine's connection as a server
		// numServer+1 - 2*numServer: connect to another machine as a client
		// 2 paths for each pair of servers actually, anyone will work
		// 2*numServer+1 - __ : clients

		for (int i = 0; i < numServer; ++i) // include itself
			try
			{
				SocketChannel tmp = createSocketChannel("127.0.0.1", serverPortBase + i);
				if (tmp != null)
				{
					SelectionKey key = tmp.register(selector, SelectionKey.OP_READ);
					key.attach(cntNumServer++);
					connAsClient.add(key);
					//addSelKey(asClient[i].register(selector, SelectionKey.OP_READ), cntNumServer++, "asClient");
				}
			}
			catch (Exception e)
			{
			}
	}

	private static void removeClientConnection(SelectionKey key)
	{
		try
		{

		//int indx = (Integer)(key.attachment());
		//if (indx == -1)
		if (!key.isValid())
			return;
		//int replaceIndx = 2*numServer + numClient;
		//System.out.println("remove client connection, replacing " + replaceIndx + " " + indx);

		//selKeyArray.set(indx, selKeyArray.get(replaceIndx));
		//writeQueue.set(indx, writeQueue.get(replaceIndx));
		writeQueue.remove(key);
		//selKeyArray.get(indx).attach(indx);
		//key.attach(-1); need it?
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
				//System.out.println("client connection closed, read");
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
				//System.out.println("client command too long!");
				break;
			}
			single.clear();
		}

		buffer.position(0);
        	Charset charset = Charset.defaultCharset();  
        	CharsetDecoder decoder = charset.newDecoder();  
        	String s = decoder.decode(buffer).toString().trim();
		return s + ":" + serverID;

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
				//System.out.println("server connection closed, read");
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
			//System.out.println("connection closed, write");
			if ((Integer)(key.attachment()) > 2*numServer)
				removeClientConnection(key);
			//e.printStackTrace();
		}
	}

	public static void addIntoWriteQueue(SelectionKey key, String command)
	{
		if (!key.isValid())
			return;
		// -1?
		//System.out.println("addintowritequeue " + indx + " " + command);
		if (writeQueue.get(key) == null)
			writeQueue.put(key, new LinkedList<String>());
		writeQueue.get(key).add(command);
		key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
	}
	
	public static String popFromWriteQueue(SelectionKey key)
	{
		if (key.isValid() && writeQueue.get(key).size() == 1)
			key.interestOps(key.interestOps() ^ SelectionKey.OP_WRITE);
		String command = writeQueue.get(key).remove();
		//System.out.println("popfromwritequeue " + indx + " " + command);
		return command;
	}

	public static void addSelKey(SelectionKey key, int x)
	{
		key.attach(x);
		// i don't even know if we need attachment any more.
			/*
		if (x >= selKeyArray.size())
			selKeyArray.add(key);
		else
			selKeyArray.set(x, key);
			*/
	}

	public static void main(String[] args) 
	{
		serverID = Integer.parseInt(args[0]);
		numServer = Integer.parseInt(args[1]);
		numMajority = numServer / 2 + 1;
		//System.out.println("Server No." + serverID + " launched.");
		//for (int i = 0; i < 2*numServer + MaxClientNum + 1; ++i)
		//	writeQueue.add(new LinkedList<String>());

		try 
		{

		selector = Selector.open();
		ServerSocketChannel listenChannel_server = createServerSocketChannel(serverPortBase + serverID);
		addSelKey(listenChannel_server.register(selector, SelectionKey.OP_ACCEPT), 0);
		ServerSocketChannel listenChannel_client = createServerSocketChannel(clientPortBase + serverID);
		addSelKey(listenChannel_client.register(selector, SelectionKey.OP_ACCEPT), 1);

		connectToOtherServer();//listenChannel_server);
		//System.out.println("registration done");
		newRoundInit();

		while (true) 
		{
			int numChanged;
			try
		    	{
				numChanged = selector.select(MaxWaitingSelectTime);
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
			//System.out.println("------ one selection --------");
			//Thread.sleep(7000);
			long cntTime = System.currentTimeMillis();
			if (numChanged == 0 && proposed)
			{
				//System.out.println("waited so long, trying to propose again");
				newRoundInit();
			}
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
		//System.out.println("processing key indx = " + indx);
	    	if (selKey.isValid() && selKey.isAcceptable())
		{
			//System.out.println("acceptable");
			if (indx == 1 && cntNumClient < MaxClientNum) 
	    		{
				SocketChannel newConn = ((ServerSocketChannel)selKey.channel()).accept();
				//System.out.println("new client connection: " + newConn.socket().getInetAddress() + " " + newConn.socket().getPort());
				newConn.configureBlocking(false); 
				addSelKey(newConn.register(selector, SelectionKey.OP_READ), (++numClient) + 2*cntNumServer);
			}
			if (indx == 2 && cntNumServer < MaxServerNum) 
			{
			}
	    	}
	    	if (selKey.isValid() && selKey.isReadable()) 
		{
			String command;
			if (indx <= 2*numServer)
				command = readFromSocketChannel(selKey);
			else
				command = readFromClientSocketChannel(selKey);
			//System.out.println("readable " + command);

			if (indx <= 2*numServer) // read from another server
			{
				int flyingInsID = Integer.parseInt(getField(command, -1));
				//System.out.println("flying instance ID = " + flyingInsID + " " + cntInsID + " " + highestInsID + " " + stateMachine.nextProcessInsID);
				//if (flyingInsID > cntInsID)
				//{
				//	System.out.println("Some One is Falling Behind!"); 
				//}
				if (command.startsWith("prepare"))
				{
					if (Math.random() < PrepareLostRate)
						return;
					checkIfAskMissedInstance(flyingInsID);
					int propNum = Integer.parseInt(getField(command, 1));
					if (propNum <= highestRespondedPropNum.getInt(flyingInsID))
						return;
					highestRespondedPropNum.put(flyingInsID, propNum);
					addIntoWriteQueue(selKey, extendCommand(flyingInsID, "re-prepare " + propNum + " " + highestAcceptedPropNum.getInt(flyingInsID) + " " + highestAcceptedValue.getStr(flyingInsID)));
				}
				else if (command.startsWith("re-prepare"))
				{
					if (Math.random() < RePrepareLostRate)
						return;
					checkIfAskMissedInstance(flyingInsID);
					numPrepareResponse.put(flyingInsID, numPrepareResponse.getInt(flyingInsID) + 1);
					int propNum = Integer.parseInt(getField(command, 1));
					int highestAcceptedNum = Integer.parseInt(getField(command, 2));
					if (highestAcceptedNum > highestRePrepareNum.getInt(flyingInsID))
					{
						highestRePrepareNum.put(flyingInsID, propNum);
						highestRePrepareValue.put(flyingInsID, getField(command, 3)); 
					}
					if (numPrepareResponse.getInt(flyingInsID) == numMajority)
					{
						String tmp;
						if (highestRePrepareNum.getInt(flyingInsID) == -1)
							tmp = extendCommand(flyingInsID, "accept " + propNum + " " + getCntRequest());
							//distinProposer.put(flyingInsID, 1);
						else
							tmp = extendCommand(flyingInsID, "accept " + propNum + " " + highestRePrepareValue.getStr(flyingInsID));
							//if (getCntRequest().equals(highestRePrepareValue.getStr(flyingInsID)))
							//	distinProposer.put(flyingInsID, 1);

						broadcastToAllServers(tmp);
						//for (int i = 1; i <= numServer; ++i)
						//	addIntoWriteQueue(i, tmp);
					}
				}
				else if (command.startsWith("accept"))
				{
					if (Math.random() < AcceptLostRate)
						return;
					checkIfAskMissedInstance(flyingInsID);
					int propNum = Integer.parseInt(getField(command, 1));
					String propValue = getField(command, 2);
					if (propNum < highestRespondedPropNum.getInt(flyingInsID))
						addIntoWriteQueue(selKey, extendCommand(flyingInsID, "re-accept rej " + propNum + " " + propValue));
					else
					{
						highestAcceptedPropNum.put(flyingInsID, propNum);
						highestAcceptedValue.put(flyingInsID, getField(command, 2));
						addIntoWriteQueue(selKey, extendCommand(flyingInsID, "re-accept accept " + propNum + " " + propValue));
					}
				}
				else if (command.startsWith("re-accept"))
				{
					if (Math.random() < ReAcceptLostRate)
						return;
					checkIfAskMissedInstance(flyingInsID);
					int propNum = Integer.parseInt(getField(command, 2));
					String propValue = getField(command, 3);
					if (command.startsWith("re-accept rej"))
					{
						//since we're not doing anything, it's possible to avoid sending it in the beginning
						//good for observation though
					}
					else
					{
						numAccepted.put(flyingInsID, numAccepted.getInt(flyingInsID) + 1);
						if (numAccepted.getInt(flyingInsID) == numMajority)
							broadcastToAllServers(extendCommand(flyingInsID, "chosen " + getField(command, 3)));
							//for (int i = 1; i <= numServer; ++i)
							//	addIntoWriteQueue(i, extendCommand(flyingInsID, "chosen " + getField(command, 3)));
					}
				}
				else if (command.startsWith("chosen"))
				{
					if (Math.random() < ChosenLostRate)
						return;
					checkIfAskMissedInstance(flyingInsID);

					String value = getField(command, 1);
					stateMachine.input(flyingInsID, value);
					checkPendingAnswer();

					if (flyingInsID > highestInsID)
						highestInsID = flyingInsID;
					//if (distinProposer.getInt(flyingInsID) == 1 && sentToClient.getInt(flyingInsID) == 0)
						//sentToClient.put(flyingInsID, 1);
					newRoundInit();
				}
				else if (command.startsWith("ask")) 
				{
					if (Math.random() < AskLostRate)
						return;
					checkIfAskMissedInstance(flyingInsID);
					int askingInsID = Integer.parseInt(getField(command, 1));
					addIntoWriteQueue(selKey, extendCommand(askingInsID, "answer " + stateMachine.getInput(askingInsID)));
				}
				else if (command.startsWith("answer"))
				{
					if (Math.random() < AnswerLostRate)
						return;
					checkIfAskMissedInstance(flyingInsID);
					String answer = getField(command, 1);
					int askingInsID = Integer.parseInt(getField(command, -1));
					if (!answer.equals("none"))
					{
						stateMachine.input(askingInsID, answer);
						checkPendingAnswer();
						if (askingInsID > highestInsID) 
							highestInsID = askingInsID; 
					} 
				}
			}
			else // read from a real client
			{
				if (command.length() > 0)
					newClientRequest(new ClientCommand(command, selKey));
			}
	    	}
	    	if (selKey.isValid() && selKey.isWritable()) 
		{
			//System.out.println("writeable");
			SocketChannel sChannel = (SocketChannel)selKey.channel();
			writeToSocketChannel(selKey, popFromWriteQueue(selKey));

			if (indx <= 2 * numServer) // write to another server
			{
			}
			else if (selKey.isValid()) // write to a real client 
			{
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

	private static class PendingAnswer
	{
		private ClientCommand clientCommand;
		private int insID;

		PendingAnswer(ClientCommand clientCommand, int insID)
		{
			this.clientCommand = clientCommand;
			this.insID = insID;
		}
	}
}
