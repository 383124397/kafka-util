package com.dante.zg.kafka;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import scala.collection.JavaConversions;

/**
 * @ClassName: KafkaProducerUtil
 * @Description: kafka-client-producer version:2.10_0.8.2.1
 * @Author dante.zg
 * @Date 2015年11月18日 下午3:37:58
 */
public class ProducerUtil {

	private Properties p;

	private ProducerConfig producerConfig;

	private Producer<String, String> producer;

	private List<KeyedMessage<String, String>> messageList;

	/**
	 * 构造方法，用于初始化client
	 * @param zkCluster: ZK cluster 【hostname:port】
	 * @param kafkaCluster: Kafka cluster 【hostname:port】
	 * @param isAsync: sync or async
	 * @throws Exception
	 */
	public ProducerUtil(String zkCluster, String kafkaCluster, boolean isAsync) throws Exception {
		if (StringUtils.isEmpty(zkCluster) || StringUtils.isEmpty(kafkaCluster)) {
			throw new Exception("Null Input, please check the params!");
		}
		p = new Properties();
		p.put("zk.connect", zkCluster);
		p.put("metadata.broker.list", kafkaCluster);
		p.put("zk.connectiontimeout", "6000");
		if (isAsync) {// 异步
			p.put("producer.type", "async");
		}
//		p.put("acks", "0");ack机制保证每次成功的put
		p.put("serializer.class", "kafka.serializer.StringEncoder");
		producerConfig = new ProducerConfig(p);
		producer = new Producer<>(producerConfig);
	}

	/**
	 * 发送消息方法
	 * @param topic: topic
	 * @param message: message
	 * @throws Exception
	 */
	public void sendMessage(final String topic, String message) throws Exception {
		if (StringUtils.isEmpty(topic) || StringUtils.isEmpty(message)) {
			throw new Exception("Null Input, please check the params!");
		}
		KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(topic, UUID.randomUUID().toString(), message);
		messageList = new ArrayList<>();
		messageList.add(keyedMessage);
		producer.send(JavaConversions.asScalaBuffer(messageList));
	}
}
