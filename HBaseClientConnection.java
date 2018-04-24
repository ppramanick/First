package com.prosenjit.hbase;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.google.protobuf.ServiceException;

public class HBaseClientConnection implements Serializable{
	
	 public Configuration connect() throws IOException, ServiceException {
	        Configuration config = HBaseConfiguration.create();

	        //String path = this.getClass().getClassLoader().getResource("/opt/hbase/conf/hbase-site.xml").getPath();
	        //String path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();

	        //config.addResource(new Path(path));
	        config.set("hbase.zookeeper.quorum", "localhost");
	        config.set("hbase.zookeeper.property.clientPort", "2181");
	        config.set("zookeeper.znode.parent", "/hbase-unsecure");
	        config.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
	        return config;
	        
	        /*try {
	            HBaseAdmin.checkHBaseAvailable(config);
	            return config;
	        } catch (MasterNotRunningException e) {
	            System.out.println("HBase is not running." + e.getMessage());
	            return null;
	        }*/

	    }

}
