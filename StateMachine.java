import java.util.HashMap;

public class StateMachine
{
	public int nextProcessInsID = 1; 

	HashMap<Integer, String> inputs;

	StateMachine()
	{
		inputs = new HashMap<Integer, String>();
	}

	/*
	public void clear()
	{
		nextProcessInsID = 1;
		inputs.clear();
	}
	*/

	public void input(int instanceID, String consensus) 
	{
		System.out.println("state machine input: " + instanceID + " " + consensus);
		inputs.put(instanceID, consensus);
		if (instanceID == nextProcessInsID)
			while (inputs.get(nextProcessInsID) != null)
			{
				// roll the machine()
				++nextProcessInsID;
			}
	}

	/*
	public void input(String consensus) 
	{
		input(nextProcessInsID, consensus);
	}
	*/

	public String getConsensus(int instanceID)
	{
		if (inputs.get(instanceID) != null)
			return inputs.get(instanceID);
		return "none";
	}

	public String getOutput()
	{
		// temp
		return inputs.get(nextProcessInsID-1) + "\n";
	}
}
