import java.util.HashMap;

public class ExtendedHashMap<K, V> extends HashMap<K, V>
{
	private int defaultInt;
	private String defaultStr;

	public ExtendedHashMap(int v)
	{
		super();
		defaultInt = v;
	}
	
	public ExtendedHashMap(String v)
	{
		super();
		defaultStr = v;
	}

	public int getInt(int indx)
	{
		Object tmp = super.get(indx);
		if (tmp == null)
			return defaultInt;
		return (Integer)tmp;
	}
	
	public String getStr(int indx)
	{
		Object tmp = super.get(indx);
		if (tmp == null)
			return defaultStr;
		return (String)tmp;
	}
}
