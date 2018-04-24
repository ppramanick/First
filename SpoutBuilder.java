package com.prosenjit.storm.spout;

import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;

import com.prosenjit.storm.Keys;

import kafka.server.KafkaConfig;

import java.util.Properties;
import java.util.UUID;

//import storm.kafka.*;


public class SpoutBuilder {
	
	public Properties configs = null;
	
	public SpoutBuilder(Properties configs) {
		this.configs = configs;
	}
	public KafkaSpout buildKafkaSpout() {
		BrokerHosts hosts = new ZkHosts(configs.getProperty(Keys.KAFKA_ZOOKEEPER));
		//BrokerHosts hosts ="127.0.0.1";
		String topic = configs.getProperty(Keys.KAFKA_TOPIC);
		//String topic ="sampleTopic"; 
		String zkRoot = configs.getProperty(Keys.KAFKA_ZKROOT);
		String groupId = configs.getProperty(Keys.KAFKA_CONSUMERGROUP);
		//SpoutConfig spoutConfig = new SpoutConfig(hosts, topic,"/" + zkRoot, groupId);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic,"/" + topic, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		//spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
		//spoutConfig.securityProtocol="PLAINTEXTSASL";
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		
		return kafkaSpout;
	}
}
