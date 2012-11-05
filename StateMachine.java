import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

public class StateMachine
{
	public int nextProcessInsID = 1; 
	public int highestInsID = 0; 

	HashMap<Integer, String> inputs;
	HashMap<Integer, String> outputs;
	HashMap<String, Boolean> dataStatus;	// true: locked, false: unlocked

	public StateMachine()
	{
		inputs = new HashMap<Integer, String>();
		outputs = new HashMap<Integer, String>();
		dataStatus = new HashMap<String, Boolean>();
	}
	
	private String getVariable(String cmd) {
		int left = cmd.indexOf('(');
		int right = cmd.indexOf(')');
		
		return cmd.substring(left + 1, right).trim();
	}
	
	private String commitCommand(String cmd) {
		
		String var;
		String result;
		Boolean status;
		
		cmd.trim();
		if (cmd.startsWith("lock")) {
			var = getVariable(cmd);
			
			if (dataStatus.get(var) == null) {
				dataStatus.put(var, true);
				result = cmd + " success."; 
			} else {
				status = dataStatus.get(var);
				if (!status) {
					dataStatus.put(var, true);
					result = cmd + " success.";
				} else {
					result = cmd + " failed. " + var + " is already locked.";
				}
			}
		} else if (cmd.startsWith("unlock")) {
			var = getVariable(cmd);
			
			if (dataStatus.get(var) == null) {
				dataStatus.put(var, false);
				result = var + " is not locked.";
			} else {
				status = dataStatus.get(var);
				if (status) {
					result = cmd + " success.";
					dataStatus.put(var, false);
				} else
					result = var + " is not locked.";
			}
		} else if (cmd.startsWith("status")) {
			Iterator iter = dataStatus.entrySet().iterator();
			StringBuilder str = new StringBuilder(1024);
			while (iter.hasNext()) {
				Entry entry = (Entry)iter.next();
				str.append(entry.getKey() + "\t");
				if ((Boolean)entry.getValue())
					str.append("locked\n");
				else
					str.append("unlocked\n");
			}
			result = str.toString();
		} else
			result = "Unknown command. Ignore.";
		
		return result;
	}

	public void input(int instanceID, String consensus) 
	{
		System.out.println("   state machine input: #" + instanceID + " " + consensus);
		String result;
		
		if (instanceID > highestInsID)
			highestInsID = instanceID;
		inputs.put(instanceID, consensus);
		if (instanceID == nextProcessInsID)
			while (inputs.get(nextProcessInsID) != null)
			{
				// roll the machine()
				result = commitCommand(inputs.get(nextProcessInsID));
				outputs.put(nextProcessInsID, result);
				System.out.println("   state machine output: #" + nextProcessInsID + " " + result);
				++nextProcessInsID;
			}
		System.out.println("   highest " + highestInsID + " nextProcessID " + nextProcessInsID);
		for (int i = 1; i <= highestInsID; ++i)
			System.out.print(inputs.get(i) + "\t");
		System.out.println();
	}

	public String getInput(int instanceID)
	{
		if (inputs.get(instanceID) != null)
			return inputs.get(instanceID);
		return "none";
	}

	public String getOutput(int instanceID)
	{
		// temp
		//return inputs.get(instanceID) + "\n";
		
		return outputs.get(instanceID);
	}
}
