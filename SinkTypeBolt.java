package com.prosenjit.storm.bolt;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.prosenjit.hbase.HBaseClientConnection;
import com.prosenjit.storm.Topology;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;


public class SinkTypeBolt extends BaseRichBolt{


	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private HTable table;
	
	public void execute(Tuple tuple) {
		String value = tuple.getString(0);
		//Long value = tuple.getLong(0);
		System.out.println("Received in SinkType bolt : "+value);
		
		Put dumpData =new Put(Bytes.toBytes(value.split(",")[0]));
		dumpData.add(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes(value.split(",")[0]));
		dumpData.add(Bytes.toBytes("cf"), Bytes.toBytes("samdata"), Bytes.toBytes(value.split(",")[1]));
		try {
			table.put(dumpData);
			table.flushCommits();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("TABLE DUMP EXCEPTION : "+e.getMessage());
			e.printStackTrace();
		}

		//collector.emit(Topology.HDFS_STREAM,new Values(value));
		//collector.emit(new Values(value));
		collector.ack(tuple);	
	}


	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		try {
			HBaseClientConnection hbaseConnection = new HBaseClientConnection();
			Connection connection = ConnectionFactory.createConnection(hbaseConnection.connect());
			final TableName table1 = TableName.valueOf("events");
			Admin admin = connection.getAdmin();
			if (admin.tableExists(table1)) {
                System.out.println("Table Exists ......");
                table =new HTable(hbaseConnection.connect(), table1);
            }
			admin.close();
		}
		catch(Exception ex) {
			System.out.println("EXCEPTION IN HBASE CONNECT : " + ex.getMessage());
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declareStream(Topology.HDFS_STREAM, new Fields( "content" ));
		//declarer.declare(new Fields("content"));
	}

}
