package processing;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Generate some standard methods to work with hbase
 * @author leandro.mora
 * @version 1.0
 * In the following version we should change the MAX function for one using coprocessor
 */
public class DBManipulator {
	
	/**
	 * Creates an HBase Configuraiton Object
	 * @param HBASE_PROP_PATH
	 * @return
	 */
	public Configuration setHbaseConfiguration(String HBASE_PROP_PATH){
		Configuration config = HBaseConfiguration.create();
        config.addResource(new Path(HBASE_PROP_PATH));   
        return config;
	}
	
	/**
	 * Generates and Hbase Connection
	 * @param config
	 * @param HBASE_TABLE_NAME
	 * @return
	 * @throws IOException
	 */
	public HTable connectToHbaseTable(Configuration config,String HBASE_TABLE_NAME) throws IOException{
		HTable table = null;     
		table = new HTable(config, HBASE_TABLE_NAME);
		return table;
	}
	
	/**
	 * Adds a Column family to a particular table
	 * @param tableName
	 * @param config
	 * @param newColumnFamilyName
	 * @throws IOException
	 */
	public void AddColumnFamily(String tableName, Configuration config, String newColumnFamilyName) throws IOException{
		HBaseAdmin admin = new HBaseAdmin(config);
		HColumnDescriptor columnFamilyDescriptor = new HColumnDescriptor(newColumnFamilyName);
		
		// Cannot edit a stucture on an active table.
		admin.disableTable(tableName);		 
		admin.addColumn(tableName, columnFamilyDescriptor);		 
		// For reading, it needs to be re-enabled.
		admin.enableTable(tableName);		
		admin.close();		
	}
	
	/**
	 * Created a standard way to generate DateTime Information.
	 * @return
	 */
	public String getDateTime(){
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		Calendar cal = Calendar.getInstance();
		return sdf.format(cal.getTime());
	}
	
	/**
	 * You can restrict the domain table using the scan object.
	 * @param table
	 * @param columFamily
	 * @param column
	 * @param s
	 * @return
	 * @throws IOException
	 */
	public int getMaxValue(HTable table, String columFamily, String column, Scan s) throws IOException{
		ResultScanner re = table.getScanner(s);
		int maxVal =0;         
		try{
		    for (Result rr = re.next(); rr != null; rr = re.next()) {
		        byte [] row = rr.getRow();
		        Get g = new Get(row);
		        g.addColumn(Bytes.toBytes(columFamily), Bytes.toBytes(column));
		        Result r = table.get(g);                   
		        String rn = Bytes.toString(r.value());
		        int temp = Integer.parseInt(rn);
		        if(maxVal < temp)
		        maxVal = temp;
		    }
		}
		finally {
		    re.close();
		}
		return maxVal;
	}
   
}
