package com.dante.zg.kafka;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * @ClassName: KafkaProducerUtil
 * @Description: kafka-client-consumer version:2.10_0.8.2.1
 * @Author dante.zg
 * @Date 2015年11月18日 下午3:37:58
 */
public class ConsumerUtil {

	private Properties p;

	private String topic;

	private Integer partitions = 1;

	private ConsumerConfig consumerConfig;

	private ConsumerConnector consumerConnector;

	private Integer blockingQueueSize = 1000000;

	private volatile BlockingQueue<String> bq;

	/**
	 * 构造方法，用于初始化client和启动监听线程
	 * @param zkCluster: ZK cluster 【hostname:port】
	 * @param groupId: group id of consumer
	 * @param topic: topic
	 * @param partitions: kafka partitions
	 * @param size: local blocking queue's init size
	 * @throws Exception
	 */
	public ConsumerUtil(String zkCluster, String groupId, String topic,
			Integer partitions, Integer size) throws Exception {
		if (StringUtils.isEmpty(zkCluster) || StringUtils.isEmpty(zkCluster) || StringUtils.isEmpty(topic)) {
			throw new Exception("Null Input, please check the params!");
		}
		this.topic = topic;
		if (partitions>1) {// 至少一个分区个数
			this.partitions = partitions;
		}
		if (size > 1) {// 至少1的size
			this.blockingQueueSize = size;
		}
		p = new Properties();
		p.put("zookeeper.connect", zkCluster);
		p.put("zookeeper.connectiontimeout.ms", "1000000");
		p.put("group.id", groupId);
		consumerConfig = new ConsumerConfig(p);
		consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
		bq = new ArrayBlockingQueue<>(blockingQueueSize);
		// 启动监听线程
		startMonitorThreads();
	}

	/**
	 * 启动kafka消费监听线超方法
	 */
	private void startMonitorThreads() {
		System.out.println("Monitor thread: " + Thread.currentThread().getName() + " started ...");
		// 创建topic和partitions映射map
		Map<String, Integer> topicCountmap = new HashMap<>();
		topicCountmap.put(topic, partitions);
		// 根据以上map创建消息流映射map
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreamsMap = consumerConnector.createMessageStreams(topicCountmap);
		// 获取所有的分区的消息流列表, list size = partitions number
		List<KafkaStream<byte[], byte[]>> kafkaSteamList = messageStreamsMap.get(topic);
		// 根据分区大小初始化监听线程池
		ExecutorService executor = Executors.newFixedThreadPool(partitions);
		// 循环启动监听线程
		for (final KafkaStream<byte[], byte[]> kafkaStream : kafkaSteamList) {
			executor.submit(new Runnable() {
				@Override
				public void run() {
					for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : kafkaStream) {
						String message = new String(messageAndMetadata.message());
						try {
							bq.put(message);// 阻塞放入
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			});
		}
	}

	/**
	 * 从本地blockingQueue中获取消息
	 * @return
	 */
	public String getMessageFromBlockingQueue() {
		String message = null;
		try {
			if (bq.isEmpty()) {// 避免使用size==0判断，影响性能
				System.out.println("the current blockingQueue size is 0, let's wait for while ...");
				Thread.sleep(5000);// 默认等待5秒，迭代进入
				getMessageFromBlockingQueue();
			} else {
//				message = bq.take();// 阻塞等待
				message = bq.poll(1, TimeUnit.SECONDS);// 默认等待1秒，超时不候
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return message;
	}
}
