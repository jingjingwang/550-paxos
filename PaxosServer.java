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
import java.io.FileNotFoundException;
import java.io.File;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.net.ConnectException;

public class PaxosServer 
{
	// ----- the configuration section, all the things that you might want to change are below -----

	// serverPortBase: each node in Paxos has a ServerSocket, which is for other servers connecting
	// it. The base number is serverPortBase, where server 0 has 2139, server 1 has 2140, server 2
	// has 2141, and so on. If a server wants to connect to another server with serverID = i, it will
	// connect to 127.0.0.1:(serverPortBase + i).
	private static final int serverPortBase = 2139;

	// clientPortBase: each node in Paxos has a ServerSocket, which is for clients connecting
	// it. The base number is clientPortBase, where server 0 has 4139, server 1 has 4140, server 2
	// has 4141, and so on. If a client wants to connect to a server with serverID = i, it will
	// use: "telnet 127.0.0.1 (clientPortBase + i)".
	private static final int clientPortBase = 4139;

	// MaxClientNum: the maximum number of clients connected to this server
	private static final int MaxClientNum = 100;

	// MaxServerNum: the maximum number of servers connected to this server
	private static final int MaxServerNum = 20;

	// MaxCmdLength: the maximum length of a command 
	private static final int MaxCmdLength = 50;

	// MaxWaitingRound: the maximum number of rounds falling behind that a node can stand 
	// before starting asking other nodes
	private static final int MaxWaitingRound = 1;

	// MaxWaitingSelectTime: the timeout of a select, after that a proposer will try to propose
	// again if nothing changes during the selection
	private static final long MaxWaitingSelectTime = 50;

	// GeneralLossRate: the general package loss rate. 0.4 means 40% possibility to lose a package 
	private static final double GeneralLossRate = 0.4;

	// Following are different loss rate for different messages. Change them if you want a more tricky test!
	private static final double PrepareLossRate = GeneralLossRate;
	private static final double RePrepareLossRate = GeneralLossRate;
	private static final double AcceptLossRate = GeneralLossRate;
	private static final double ReAcceptLossRate = GeneralLossRate;
	private static final double ChosenLossRate = GeneralLossRate;
	private static final double AskLossRate = GeneralLossRate;
	private static final double AnswerLossRate = GeneralLossRate;

	// ----- the configuration section, all the things that you might want to change are above -----

	// its unique server ID
	private static int serverID;
	// total number of servers, given when launching
	private static int numServer;
	// the number of majority, derived from numServer
	private static int numMajority;
	// if it has proposed for this instance
	private static boolean proposed = false;
	// the current proposal number
	private static int cntPropNum = 0;
	// the current instance ID
	private static int cntInsID = 1;
	// the current number of connected clients
	private static int cntNumClient = 0;
	// the current number of connected servers
	private static int cntNumServer = 0;
	// the current ID of the next client, since we need to distinguish clients because they own locks
	private static int localClientID = 0;
	// the highetst instance ID that it has seen, will help decide the next instance ID
	private static int highestInsID = 1;

	// states that a Paxos node should keep
	private static ExtendedHashMap<Integer, Integer> numAccepted = new ExtendedHashMap<Integer, Integer>(0);
	private static ExtendedHashMap<Integer, Integer> numPrepareResponse = new ExtendedHashMap<Integer, Integer>(0);
	private static ExtendedHashMap<Integer, Integer> highestAcceptedPropNum = new ExtendedHashMap<Integer, Integer>(-1);
	private static ExtendedHashMap<Integer, String> highestAcceptedValue = new ExtendedHashMap<Integer, String>("");
	private static ExtendedHashMap<Integer, Integer> highestRePrepareNum = new ExtendedHashMap<Integer, Integer>(-1);
	private static ExtendedHashMap<Integer, String> highestRePrepareValue = new ExtendedHashMap<Integer, String>("");
	private static ExtendedHashMap<Integer, Integer> highestRespondedPropNum = new ExtendedHashMap<Integer, Integer>(-1);
	
	// the state machine
	private static StateMachine stateMachine = new StateMachine();
	// the file for outputting logs
	private static BufferedWriter debuggingLog;
	// loggingLevel, 0 is the most important one. logs having level <= loggingLevel will be outputted.
	private static int loggingLevel = 1;

	// the non-blocking stuff
	private static Selector selector;
	// the queue of client requests
	private static LinkedList<ClientCommand> clientRequestQueue = new LinkedList<ClientCommand>();
	// the set of SelectionKeys when connecting to other servers as a client. need them for broadcasting message
	private static HashSet<SelectionKey> connAsClient = new HashSet<SelectionKey>();
	// the set of client connections where their requests have been chosen but the responses are not ready yet
	private static LinkedList<PendingAnswer> pendingToAnswer = new LinkedList<PendingAnswer>();
	// the write queue for each socket
	private static HashMap<SelectionKey, LinkedList<String> > writeQueue = new HashMap<SelectionKey, LinkedList<String> >();

	private static void outputDebuggingInfo(String str, int level)
	{
		try
		{

		if (level == 0)
			System.out.println(str);
		if (level <= loggingLevel)
		// 0 is the most important one
		{
			debuggingLog.write(str);
			debuggingLog.newLine();
			debuggingLog.flush();
		}

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private static void newRoundInit()
	{
		// init for a new round
		outputDebuggingInfo("newRoundInit", 1);
		if (stateMachine.getInput(cntInsID).equals("none")) 
		{
			// hasn't had a chosen value, not a new round, so just update proposer's state
			numAccepted.put(cntInsID, 0);
			numPrepareResponse.put(cntInsID, 0);
			cntPropNum += (new Random()).nextInt(10) + 1;
		}
		else 
		{
			// has result for this instance already, could go to a new one
			if (getCntRequest() != null && stateMachine.getInput(cntInsID).equals(getCntRequest())) 
			{
				// the request is successfully chosen, move to the next request
				outputDebuggingInfo("add pendig answer " + cntInsID + " " + clientRequestQueue.get(0).command, 2);
				pendingToAnswer.add(new PendingAnswer(clientRequestQueue.get(0), cntInsID));
				// not needed anymore
				clientRequestQueue.remove();
				checkPendingAnswer();
			}
			// highestInsID: highest instance ID that I know via "chosen" or "answer"
			// to guarantee there won't be an empty instance id with no value chosen
			cntInsID = highestInsID + 1;
			cntPropNum = (new Random()).nextInt(10) + 1;
		}

		proposed = false;
		tryPropose();
	}

	public static void checkPendingAnswer()
	{
		outputDebuggingInfo("checkpendinganswer " + pendingToAnswer.size(), 2);
		for (int i = 0; i < pendingToAnswer.size(); )
		{
			int tmpID = pendingToAnswer.get(i).insID;
			String tmpAns = stateMachine.getOutput(tmpID);
			if (tmpAns != null)
			{
				// could respond to this pending client
				addIntoWriteQueue(pendingToAnswer.get(i).clientCommand.key, tmpAns + '\n'); 
				pendingToAnswer.remove(i);
			}
			else
				++i;
		}
	}

	public static void broadcastToAllServers(String str)
	{
		Iterator<SelectionKey> iter = connAsClient.iterator();
		while (iter.hasNext())
			addIntoWriteQueue(iter.next(), str);
	}

	public static void checkIfAskMissedInstance(int flyingInsID)
	{
		// if it is falling behind for a certain number of iterations
		if (flyingInsID - stateMachine.nextProcessInsID >= MaxWaitingRound)
		{
			outputDebuggingInfo("asking from " + stateMachine.nextProcessInsID + " to " + (flyingInsID-1), 2);
			for (int i = stateMachine.nextProcessInsID; i < flyingInsID; ++i)
				if (stateMachine.getInput(i).equals("none"))
					broadcastToAllServers(extendCommand(cntInsID, "ask " + i));
		}
	}

	public static String getCntRequest()
	{
		if (clientRequestQueue.size() == 0)
			return null;
		return clientRequestQueue.get(0).command;
	}

	public static void tryPropose()
	{
		outputDebuggingInfo("tryPropose", 1);
		if (clientRequestQueue.size() > 0)
		{
			outputDebuggingInfo("has sth, going to propose", 1);
			proposed = true;
			broadcastToAllServers(extendCommand(cntInsID, "prepare " + cntPropNum + " " + getCntRequest()));
		}
	}

	public static void newClientRequest(String str, SelectionKey key)
	{
		outputDebuggingInfo("new client request" , 1);
		String tmp = (String)(key.attachment());
		str = str + ":" + tmp.substring(tmp.indexOf("_")+1);
		// we've got the client request, block it until the response is sent
		key.interestOps(key.interestOps() ^ SelectionKey.OP_READ);
		clientRequestQueue.add(new ClientCommand(str, key));
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
		return ssChannel;

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public static void connectToOtherServer()
	{
		for (int i = 0; i < numServer; ++i) 
			connectToOneServer(i, true);
	}


	public static void connectToOneServer(int i, boolean justStart) 
	{
		try
		{
			// currently has to be 127.0.0.1 with a unique port number indicating the serverID
			SocketChannel tmp = createSocketChannel("127.0.0.1", serverPortBase + i);
			if (tmp != null)
			{
				// connected to an existing server
				SelectionKey key = tmp.register(selector, SelectionKey.OP_READ);
				key.attach("paxos_as_client");
				connAsClient.add(key);
				if (justStart)
					// not because of receiving a "newserver" message
					writeToSocketChannel(key, extendCommand(0, "newserver " + serverID));
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private static void removeConnection(SelectionKey key)
	{
		try
		{

		if (!key.isValid())
			return;
		if (key.attachment().equals("paxos_as_client"))
			connAsClient.remove(key);
		if (key.attachment().equals("paxos_as_server"))
			cntNumServer--;
		if (key.attachment().equals("client"))
			cntNumClient--;
		writeQueue.remove(key);
		((SocketChannel)key.channel()).close();
		key.cancel();

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private static String readFromSocketChannel(SelectionKey key)
	{
		try
		{

		SocketChannel channel = (SocketChannel)key.channel();
		ByteBuffer single = ByteBuffer.allocateDirect(1);
		ByteBuffer buffer = ByteBuffer.allocateDirect(MaxCmdLength);
		single.clear();
		buffer.clear();

		int hasRead = 0;
		while (true)
		{
			// read bytes one by one is safe
			int justRead = channel.read(single);
			if (justRead == -1) 
				throw new Exception();
			if (justRead > 0)
			{
				hasRead += justRead;
				// a client request from telnet ends with 13 & 10
				// a message sent by other servers ends with #
				if (single.get(0) == (byte)(10) || single.get(0) == (byte)('#'))
					break;
				buffer.put(single.get(0));
			}
			if (hasRead >= MaxCmdLength)
			{
				outputDebuggingInfo("command too long!", 0);
				break;
			}
			single.clear();
		}

		buffer.position(0);
        	Charset charset = Charset.defaultCharset();  
        	CharsetDecoder decoder = charset.newDecoder();  
        	String s = decoder.decode(buffer).toString().trim();
		if (single.get(0) == (byte)(10)) 
			// we need to make the requests sent by different clients on different servers different,
			// even if they look all the same like "lock(x)"
			s = s + ":" + serverID;
		return s;

		}
		catch (Exception e)
		{
			// the connection doesn't exist anymore
			outputDebuggingInfo("client connection closed, read", 1);
			removeConnection(key);
			return "";
		}
	}

	private static void writeToSocketChannel(SelectionKey key, String cmd)
	{
		if (!key.isValid())
			return;
		outputDebuggingInfo("writing " + cmd, 1);
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
			// the connection doesn't exist anymore
			outputDebuggingInfo("connection closed, write", 1);
			removeConnection(key);
		}
	}

	public static void addIntoWriteQueue(SelectionKey key, String command)
	{
		if (!key.isValid())
			return;
		outputDebuggingInfo("addintowritequeue " + command, 2);
		if (writeQueue.get(key) == null)
			writeQueue.put(key, new LinkedList<String>());
		writeQueue.get(key).add(command);
		// we are interested in OP_WRITE iff there is something to write
		key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
	}
	
	public static String popFromWriteQueue(SelectionKey key)
	{
		if (key.isValid() && writeQueue.get(key).size() == 1)
			// we are interested in OP_WRITE iff there is something to write
			key.interestOps(key.interestOps() ^ SelectionKey.OP_WRITE);
		String command = writeQueue.get(key).remove();
		outputDebuggingInfo("popfromwritequeue " + command, 2);
		return command;
	}

	public static void loadCheckpoint()
	{
		try
		{

		String filename = "550paxos-" + serverID + ".checkpoint";
		BufferedReader reader = new BufferedReader(new FileReader(new File(filename)));
		String line;
		while ((line = reader.readLine()) != null)
		{
			outputDebuggingInfo(line, 1);
			String[] items = line.split("\t");
			int insID = Integer.parseInt(items[0]);
			highestAcceptedPropNum.putInt(insID, Integer.parseInt(items[1]));
			highestAcceptedValue.putStr(insID, items[2]);
			highestRespondedPropNum.putInt(insID, Integer.parseInt(items[3]));
			highestRePrepareValue.putStr(insID, items[4]);
			highestRePrepareNum.putInt(insID, Integer.parseInt(items[5]));
			numAccepted.putInt(insID, Integer.parseInt(items[6]));
			numPrepareResponse.putInt(insID, Integer.parseInt(items[7]));
			stateMachine.input(insID, items[8]);
		}
		highestInsID = stateMachine.highestInsID;
		outputDebuggingInfo("load checkpoint done", 2);

		}
		catch (FileNotFoundException e)
		{
			// if there is no checkpoint 
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void dumpCheckpoint()
	{
		try
		{

		String filename = "550paxos-" + serverID + ".checkpoint";
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(filename)));
		for (int i = 1; i <= highestInsID; ++i)
		{
			writer.write(i + "\t" + 
				     highestAcceptedPropNum.getInt(i) + "\t" + 
				     highestAcceptedValue.getStr(i) + "\t" +
				     highestRespondedPropNum.getInt(i) + "\t" +
				     highestRePrepareValue.getStr(i) + "\t" +
				     highestRePrepareNum.getInt(i) + "\t" +
				     numAccepted.getInt(i) + "\t" +
				     numPrepareResponse.getInt(i) + "\t" +
				     stateMachine.getInput(i));
			writer.newLine();
		}
		writer.close();
		outputDebuggingInfo("dump checkpoint done", 2);

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void main(String[] args) 
	{
		try 
		{

		serverID = Integer.parseInt(args[0]);
		numServer = Integer.parseInt(args[1]);
		numMajority = numServer / 2 + 1;
		debuggingLog = new BufferedWriter(new FileWriter(new File("550paxos-" + serverID + ".debugginglog")));
		outputDebuggingInfo("Server No." + serverID + " launched.", 0);
		
		loadCheckpoint();

		selector = Selector.open();
		ServerSocketChannel listenChannel_server = createServerSocketChannel(serverPortBase + serverID);
		listenChannel_server.register(selector, SelectionKey.OP_ACCEPT).attach("listen_server");
		ServerSocketChannel listenChannel_client = createServerSocketChannel(clientPortBase + serverID);
		listenChannel_client.register(selector, SelectionKey.OP_ACCEPT).attach("listen_client");

		connectToOtherServer();
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
				// handle keys one by one
				SelectionKey selKey = keyIter.next();
				if (selKey.readyOps() > 0)
			 		processSelectionKey(selKey);
				dumpCheckpoint();
				keyIter.remove();
		    	}
			outputDebuggingInfo("------ one selection ------", 2);
			if (numChanged == 0 && proposed)
				//waited so long with no updates, trying to propose again
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
		String tag = (String)selKey.attachment();
		outputDebuggingInfo("processing key ", 2);
	    	if (selKey.isValid() && selKey.isAcceptable())
		{
			outputDebuggingInfo("acceptable", 1);
			if (tag.equals("listen_server") && cntNumServer < MaxServerNum) 
			{
				SocketChannel newConn = ((ServerSocketChannel)selKey.channel()).accept();
				newConn.configureBlocking(false); 
				newConn.register(selector, SelectionKey.OP_READ).attach("paxos_as_server"); 
				cntNumServer++;
				outputDebuggingInfo("new server: " + newConn.socket().getInetAddress() + " " + newConn.socket().getPort(), 1);
			}
			if (tag.equals("listen_client") && cntNumClient < MaxClientNum)
	    		{
				SocketChannel newConn = ((ServerSocketChannel)selKey.channel()).accept();
				newConn.configureBlocking(false); 
				newConn.register(selector, SelectionKey.OP_READ).attach("client_" + (localClientID++)); 
				cntNumClient++;
				outputDebuggingInfo("new client: " + newConn.socket().getInetAddress() + " " + newConn.socket().getPort(), 1);
			}
	    	}
	    	if (selKey.isValid() && selKey.isReadable()) 
		{
			String command = readFromSocketChannel(selKey);
			outputDebuggingInfo("readable " + command, 1);
			if (command.equals(""))
				return;

			if (tag.startsWith("paxos")) // read from another server
			{
				// the instance ID in this message, might be different from the current one
				int flyingInsID = Integer.parseInt(getField(command, -1));
				if (command.startsWith("prepare"))
				{
					if (Math.random() < PrepareLossRate)
						return;
					checkIfAskMissedInstance(flyingInsID);
					int propNum = Integer.parseInt(getField(command, 1));
					// Paxos logic	
					if (propNum <= highestRespondedPropNum.getInt(flyingInsID))
						return;
					highestRespondedPropNum.put(flyingInsID, propNum);
					addIntoWriteQueue(selKey, extendCommand(flyingInsID, "re-prepare " + propNum + " " + highestAcceptedPropNum.getInt(flyingInsID) + " " + highestAcceptedValue.getStr(flyingInsID)));
				}
				else if (command.startsWith("re-prepare"))
				{
					if (Math.random() < RePrepareLossRate)
						return;
					checkIfAskMissedInstance(flyingInsID);
					numPrepareResponse.put(flyingInsID, numPrepareResponse.getInt(flyingInsID) + 1);
					int propNum = Integer.parseInt(getField(command, 1));
					int highestAcceptedNum = Integer.parseInt(getField(command, 2));
					// Paxos logic	
					if (highestAcceptedNum > highestRePrepareNum.getInt(flyingInsID))
					{
						highestRePrepareNum.put(flyingInsID, propNum);
						highestRePrepareValue.put(flyingInsID, getField(command, 3)); 
					}
					// Paxos logic	
					if (numPrepareResponse.getInt(flyingInsID) == numMajority)
					{
						String tmp;
						if (highestRePrepareNum.getInt(flyingInsID) == -1)
							tmp = extendCommand(flyingInsID, "accept " + propNum + " " + getCntRequest());
						else
							tmp = extendCommand(flyingInsID, "accept " + propNum + " " + highestRePrepareValue.getStr(flyingInsID));
						broadcastToAllServers(tmp);
					}
				}
				else if (command.startsWith("accept"))
				{
					if (Math.random() < AcceptLossRate)
						return;
					checkIfAskMissedInstance(flyingInsID);
					int propNum = Integer.parseInt(getField(command, 1));
					String propValue = getField(command, 2);
					// Paxos logic	
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
					if (Math.random() < ReAcceptLossRate)
						return;
					checkIfAskMissedInstance(flyingInsID);
					int propNum = Integer.parseInt(getField(command, 2));
					String propValue = getField(command, 3);
					if (command.startsWith("re-accept rej"))
					{
						//although it's useless, we're still keeping it for observation
					}
					else
					{
						numAccepted.put(flyingInsID, numAccepted.getInt(flyingInsID) + 1);
						// Paxos logic	
						if (numAccepted.getInt(flyingInsID) == numMajority)
							broadcastToAllServers(extendCommand(flyingInsID, "chosen " + getField(command, 3)));
					}
				}
				else if (command.startsWith("chosen"))
				{
					if (Math.random() < ChosenLossRate)
						return;
					checkIfAskMissedInstance(flyingInsID);
					
					String value = getField(command, 1);
					// got a new chosen value
					stateMachine.input(flyingInsID, value);
					checkPendingAnswer();

					if (flyingInsID > highestInsID)
						highestInsID = flyingInsID;
					// try a new round
					newRoundInit();
				}
				else if (command.startsWith("ask")) 
				{
					if (Math.random() < AskLossRate)
						return;
					checkIfAskMissedInstance(flyingInsID);
					int askingInsID = Integer.parseInt(getField(command, 1));
					addIntoWriteQueue(selKey, extendCommand(askingInsID, "answer " + stateMachine.getInput(askingInsID)));
				}
				else if (command.startsWith("answer"))
				{
					if (Math.random() < AnswerLossRate)
						return;
					checkIfAskMissedInstance(flyingInsID);
					String answer = getField(command, 1);
					int askingInsID = Integer.parseInt(getField(command, -1));
					if (!answer.equals("none"))
					{
						// got a new chosen value
						stateMachine.input(askingInsID, answer);
						checkPendingAnswer();
						if (askingInsID > highestInsID) 
							highestInsID = askingInsID; 
					} 
				}
				else if (command.startsWith("newserver"))
				{
					// connected by a new node as a server
					checkIfAskMissedInstance(flyingInsID);
					int id = Integer.parseInt(getField(command, 1));
					// try to connect to that new node
					connectToOneServer(id, false);
				}
			}
			else // read from a real client
				if (command.length() > 0)
					newClientRequest(command, selKey);
	    	}
	    	if (selKey.isValid() && selKey.isWritable()) 
		{
			outputDebuggingInfo("writeable", 2);
			SocketChannel sChannel = (SocketChannel)selKey.channel();
			writeToSocketChannel(selKey, popFromWriteQueue(selKey));
			// we are interested in OP_READ again if just wrote to a client socket
			selKey.interestOps(selKey.interestOps() | SelectionKey.OP_READ);
	    	}
	}

	public static String extendCommand(int insID, String s)
	{
		s = s + " " + insID + "#";
		outputDebuggingInfo("extending command " + s, 2);
		return s;
	}

	public static String getField(String s, int indx)
	{
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
