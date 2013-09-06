package processing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;

public class Process_2 {

	private static Configuration conf;
	private static HTablePool pool;


	 
	/**
	 * Creates the tables and table columns in the database.
	 * 
	 * @throws IOException
	 */
	
	public static void initDatabase() throws IOException {
	 String blogsTable="Blogs";
	 String blogColumnFamily="Sports";
	 HBaseAdmin admin = new HBaseAdmin(conf);
	 HTableDescriptor[] blogs = admin.listTables(blogsTable);
	 //HTableDescriptor[] users = admin.listTables(usersTable);
	 
	 if (blogs.length == 0) {
	  HTableDescriptor blogstable = new HTableDescriptor(blogsTable);
	  admin.createTable(blogstable);
	  // Cannot edit a stucture on an active table.
	  admin.disableTable(blogsTable);
	 
	  HColumnDescriptor blogdesc = new HColumnDescriptor(blogColumnFamily);
	  admin.addColumn(blogsTable, blogdesc);
	 
	  HColumnDescriptor commentsdesc = new HColumnDescriptor("comments");
	  admin.addColumn(blogsTable, commentsdesc);
	 
	  // For readin, it needs to be re-enabled.
	  admin.enableTable(blogsTable);
	 }
	 admin.close();
	 
	}
	
	public static void main(String[] args){
		conf = HBaseConfiguration.create();
		conf.clear();
		conf.set("hbase.zookeeper.quorum", "127.0.0.1");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		conf.set("hbase.master", "127.0.0.1:60000");
		 // Without pooling, the connection to a table will be reinitialized.
		 // Creating a new connection to a table might take up to 5-10 seconds!
		pool = new HTablePool(conf, 10);
		 try {
		  initDatabase();
		 } catch (IOException e) {
		 }	 	
	}
}

