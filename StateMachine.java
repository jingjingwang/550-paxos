import java.util.HashMap;

public class StateMachine
{
	public int nextProcessInsID = 1; 
	private int highestInsID = 0; 

	HashMap<Integer, String> inputs;

	StateMachine()
	{
		inputs = new HashMap<Integer, String>();
	}

	public void input(int instanceID, String consensus) 
	{
		System.out.println("   state machine input: " + instanceID + " " + consensus);
		if (instanceID > highestInsID)
			highestInsID = instanceID;
		inputs.put(instanceID, consensus);
		if (instanceID == nextProcessInsID)
			while (inputs.get(nextProcessInsID) != null)
			{
				// roll the machine()
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
		return inputs.get(instanceID) + "\n";
	}
}
