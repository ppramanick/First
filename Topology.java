package com.prosenjit.storm;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import com.google.common.collect.Maps;
import com.prosenjit.storm.bolt.BoltBuilder;
import com.prosenjit.storm.bolt.SinkTypeBolt;
import com.prosenjit.storm.spout.SpoutBuilder;


public class Topology {
	
	public Properties configs;
	public BoltBuilder boltBuilder;
	public SpoutBuilder spoutBuilder;
	public static final String HDFS_STREAM = "hdfs-stream";
	

	public Topology(String configFile) throws Exception {
		configs = new Properties();
		try {
			configs.load(Topology.class.getResourceAsStream("/default_config.properties"));
			boltBuilder = new BoltBuilder(configs);
			spoutBuilder = new SpoutBuilder(configs);
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(0);
		}
	}

	private void submitTopology() throws Exception {
		TopologyBuilder builder = new TopologyBuilder();	
		KafkaSpout kafkaSpout = spoutBuilder.buildKafkaSpout();
		SinkTypeBolt sinkTypeBolt = boltBuilder.buildSinkTypeBolt();
		//HdfsBolt hdfsBolt = boltBuilder.buildHdfsBolt();

	

		int kafkaSpoutCount = Integer.parseInt(configs.getProperty(Keys.KAFKA_SPOUT_COUNT));
		builder.setSpout(configs.getProperty(Keys.KAFKA_SPOUT_ID), kafkaSpout, kafkaSpoutCount);

		int sinkBoltCount = Integer.parseInt(configs.getProperty(Keys.SINK_BOLT_COUNT));
		builder.setBolt(configs.getProperty(Keys.SINK_TYPE_BOLT_ID),sinkTypeBolt,sinkBoltCount).shuffleGrouping(configs.getProperty(Keys.KAFKA_SPOUT_ID));

		//int hdfsBoltCount = Integer.parseInt(configs.getProperty(Keys.HDFS_BOLT_COUNT));
		//builder.setBolt(configs.getProperty(Keys.HDFS_BOLT_ID),hdfsBolt,hdfsBoltCount).shuffleGrouping(configs.getProperty(Keys.SINK_TYPE_BOLT_ID),HDFS_STREAM);

		//builder.setBolt("hbase-bolt", hbaseBolt,1).shuffleGrouping(configs.getProperty(Keys.SINK_TYPE_BOLT_ID));
		//builder.setBolt("hbase-bolt", hbaseBolt,1).fieldsGrouping(configs.getProperty(Keys.SINK_TYPE_BOLT_ID), new Fields("content"));
		
		
		Config conf = new Config();
		String topologyName = configs.getProperty(Keys.TOPOLOGY_NAME);
		//added on 20th APR
		conf.setMaxTaskParallelism(5);
        //conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 2);
        conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
        conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
        conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("127.0.0.1"));
        
		conf.setNumWorkers(1);
		
		LocalCluster cluster =new LocalCluster();
		cluster.submitTopology(topologyName, conf, builder.createTopology());
		Utils.sleep(1000);
		//cluster.killTopology(topologyName);
		//cluster.shutdown();
		//StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
	}

	public static void main(String[] args) throws Exception {
		String configFile;
		if (args.length == 0) {
			//System.out.println("Missing input : config file location, using default");
			configFile = "/opt/storm/apache-storm-1.1.0/lib/default_config.properties";
			
			
		} else{
			configFile = "/opt/storm/apache-storm-1.1.0/lib//default_config.properties";
		}
		
		Topology ingestionTopology = new Topology(configFile);
		ingestionTopology.submitTopology();
	}
}
