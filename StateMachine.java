import java.util.HashMap;

public class StateMachine
{
	public int cntInstanceID; 

	HashMap<Integer, String> inputs;

	StateMachine()
	{
		inputs = new HashMap<Integer, String>();
	}

	public void clear()
	{
		cntInstanceID = 0;
		inputs.clear();
	}

	public void input(int instanceID, String consensus) // change instanceID!
	{
		System.out.println("state machine input: " + instanceID + " " + consensus);
		inputs.put(instanceID, consensus);
		if (instanceID == cntInstanceID)
			while (inputs.get(cntInstanceID) != null)
			{
				// roll the machine()
				++cntInstanceID;
			}
	}

	public void input(String consensus) // change instanceID!
	{
		input(cntInstanceID, consensus);
	}

	public String getConsensus(int instanceID)
	{
		return inputs.get(instanceID);
	}

	public String getOutput()
	{
		// temp
		return inputs.get(cntInstanceID-1) + "\n";
	}
}
