import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

public class StateMachine
{

	// use [serverID:clientID] to uniquely address a client
	public class Client {
		int serverID;
		int clientID;
		int insNum;
		
		@Override
		public String toString() {
			return (serverID + ":" + clientID);
		}
	}
	
	// Command format: [lock(x)/unlock(x)/status]:[server id]:[client id]
	public class Command {
		static public final int LOCK = 0;
		static public final int UNLOCK = 1;
		static public final int STATUS = 2;
		static public final int UNKNOWN = 3;
		
		public String input;
		public String output;
		
		public int type;	// 0: lock, 1: unlock, 2: status, 3: unknown
		public String var;
		public Client client;
		
		public Command(String cmd, int insNum) {
			cmd.trim();
			
			input = cmd;
			output = null;
			client = new Client();
			client.insNum = insNum;
			client.serverID = parseServerID(cmd);
			client.clientID = parseClientID(cmd);
			
			if (cmd.startsWith("lock")) {
				type = Command.LOCK;
				var = parseVariable(cmd);
			} else if (cmd.startsWith("unlock")) {
				type = Command.UNLOCK;
				var = parseVariable(cmd);
			} else if (cmd.startsWith("status")) {
				type = Command.STATUS;
			} else
				type = Command.UNKNOWN;
		}
		
		private String parseVariable(String cmd) {
			int beginIndex = cmd.indexOf('(') + 1;
			int endIndex = cmd.lastIndexOf(')');
			return cmd.substring(beginIndex, endIndex).trim();
		}
		
		private int parseServerID(String cmd) {
			int beginIndex = cmd.indexOf(':') + 1;
			int endIndex = cmd.lastIndexOf(':');
			return Integer.parseInt(cmd.substring(beginIndex, endIndex));
		}
		
		private int parseClientID(String cmd) {
			int beginIndex = cmd.lastIndexOf(':') + 1;
			return Integer.parseInt(cmd.substring(beginIndex));
		}
	}
	
	public interface Visitor {
		void notify(Client c);
	}
	
	public class Lock {
		
		boolean isLocked;
		Client lockOwner;
		Visitor visitor;
		LinkedList<Client> waitingList;
		
		public Lock(Visitor v) {
			visitor = v;
			isLocked = false;
			lockOwner = null;
			waitingList = new LinkedList<Client>();
		}
		
		// require the lock
		// true: require lock success
		// false: lock is currently held, client will be appended in the waiting list
		public boolean requireLock(Client client) {
			if (!isLocked) {
				isLocked = true;
				lockOwner = client;
				return true;
			} else {
				waitingList.push(client);
				return false;
			}
		}
		
		// release the lock
		// if there are other clients in the waiting list, wake up the first one in the list by visitor
		// return: 
		// true means releasing success; 
		// false means a client tries to release a lock that he hasn't held it
		public boolean releaseLock(Client client) {
			if (client.serverID == lockOwner.serverID && client.clientID == lockOwner.clientID) {
				if (waitingList.size() > 0) {
					lockOwner = waitingList.pop();
					visitor.notify(lockOwner);
				} else {
					isLocked = false;
					lockOwner = null;
				}
				return true;
			} else
				return false;
		}
		
		@Override
		public String toString() {
			String ret;
			if (isLocked) {
				ret = "locked by client " + lockOwner;
				
				if (waitingList.size() > 0) {
					ret += ", waiting by ";
					Iterator iter = waitingList.iterator();
					Client c = (Client)iter.next();
					ret += c;
					
					while (iter.hasNext()) {
						c = (Client)iter.next();
						ret += ", " + c;
					}
				}
			} else
				ret = "unlocked";
			
			return ret;
		}
	}
	
	public int nextProcessInsID = 1; 
	public int highestInsID = 0; 

	HashMap<Integer, Command> inputs;
	HashMap<String, Lock> lockMap;	
	
	// Call back function
	// only occurs waking up a blocking lock operation
	Visitor visitor = new Visitor() {
		public void notify(Client client) {
			Command cmd = inputs.get(client.insNum);
			cmd.output = cmd.input + " success.";
			System.out.println(" state machine output: " + cmd.output);
		}
	};

	public StateMachine()
	{
		inputs = new HashMap<Integer, Command>();
		lockMap = new HashMap<String, Lock>();
	}
	
	private void commitCommand(Command cmd) {
		
		Lock lock;
		
		if (cmd.type == Command.LOCK) {
			
			if (lockMap.get(cmd.var) == null) {
				lock = new Lock(visitor);
				lockMap.put(cmd.var, lock);
				lock.requireLock(cmd.client);
				cmd.output = cmd.input + " success."; 
			} else {
				lock = lockMap.get(cmd.var);
				if (lock.requireLock(cmd.client))
					cmd.output = cmd.input + " success.";
			}
			
		} else if (cmd.type == Command.UNLOCK) {
			
			if (lockMap.get(cmd.var) == null) {
				lock = new Lock(visitor);
				lockMap.put(cmd.var, lock);
				cmd.output = cmd.var + " is not locked. Ignore.";
			} else {
				lock = lockMap.get(cmd.var);
				if (lock.releaseLock(cmd.client))
					cmd.output = cmd.input + " success.";
				else
					cmd.output = cmd.var + " is not owned by client. Ignore.";
			}
			
		} else if (cmd.type == Command.STATUS) {
			Iterator iter = lockMap.entrySet().iterator();
			StringBuilder str = new StringBuilder(1024);
			while (iter.hasNext()) {
				Entry entry = (Entry)iter.next();
				str.append(entry.getKey() + "  " + ((Lock)entry.getValue()).toString() + "\n");
			}
			cmd.output = str.toString();
		} else
			cmd.output = "Unknown command. Ignore.";
		
		if (cmd.output != null)
			System.out.println(" state machine output: " + cmd.output);
	}

	// get input from paxos server
	public void input(int instanceID, String consensus) 
	{
		if (inputs.get(instanceID) != null)
			return;
		if (instanceID > highestInsID)
			highestInsID = instanceID;
		System.out.println(" state machine input: #" + instanceID + " " + consensus);
		
		Command cmd = new Command(consensus, instanceID);
		inputs.put(instanceID, cmd);
		
		if (instanceID == nextProcessInsID)
			// run as many commands as possible
			while (inputs.get(nextProcessInsID) != null)
			{
				// roll the machine
				commitCommand(inputs.get(nextProcessInsID));
				++nextProcessInsID;
			}
		System.out.println(" nextProcessID " + nextProcessInsID);
		System.out.print(" ");
		for (int i = 1; i <= highestInsID; ++i)
			System.out.print(inputs.get(i).input + "\t");
		System.out.println();
	}

	public String getInput(int instanceID)
	{
		if (inputs.get(instanceID) != null)
			return inputs.get(instanceID).input;
		return "none";
	}

	// passively output the result to paxos server
	public String getOutput(int instanceID)
	{
		return inputs.get(instanceID).output;
	}
}
