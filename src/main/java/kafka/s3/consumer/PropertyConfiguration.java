package kafka.s3.consumer;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;

public class PropertyConfiguration extends PropertiesConfiguration implements
		Configuration {

	private static final org.slf4j.Logger logger = LoggerFactory
			.getLogger(PropertyConfiguration.class);

	// conf property names
	public static final String ZK_CONNECT_STRING = "zk.connect";
	public static final String ZK_SESSION_TIMEOUT = "zk.sessiontimeout.ms";
	public static final String ZK_SYNC_TIME       =	"zk.synctime.ms";
	public static final String CONSUMER_GROUP_ID = "KafkaConsumer.groupId";

	public static final String DEFAULT_FETCH_SIZE = "fetch.size";
	public static final String SOCKET_BUFFER_SIZE = "socket.buffersize";

	private static final int DEFAULT_S3_SIZE = 1024;
	private static final int DEFAULT_MSG_SIZE = 512;

	private static final String PROP_S3_ACCESS_KEY = "s3.accesskey";
	private static final String PROP_S3_SECRET_KEY = "s3.secretkey";
	private static final String PROP_S3_BUCKET = "s3.bucket";
	private static final String PROP_S3_PREFIX = "s3.prefix";

	private static final String PROP_S3_MAX_OBJECT_SIZE = "s3.maxobjectsize";
	private static final String PROP_S3_TOPIC_SIZES = "s3.objectsizes";

	private static final String PROP_KAFKA_MAX_MESSAGE_SIZE = "kafka.maxmessagesize";
	private static final String PROP_KAFKA_TOPICS = "kafka.topics";

	private Map<String, Integer> getConfigMap(String prop) {
		HashMap<String, Integer> result = new HashMap<String, Integer>();
		String[] fields = getStringArray(prop);
		for (String topics : fields) {
			String[] fieldPart = topics.trim().split(":");
			if (result.containsKey(fieldPart[0])) {
				throw new RuntimeException("Duplicate field " + fieldPart[0]);
			}
			result.put(fieldPart[0], Integer.valueOf(fieldPart[1]));
		}
		return result;
	}

	public PropertyConfiguration(URL propsLocation)
			throws ConfigurationException {
		super(propsLocation);
		if (isEmpty()) {
			throw new RuntimeException("Empty config");
		}
	}

	@Override
	public String getS3AccessKey() {
		String s3AccessKey = getString(PROP_S3_ACCESS_KEY);
		if (s3AccessKey == null || s3AccessKey.isEmpty()) {
			throw new RuntimeException("Invalid property " + PROP_S3_ACCESS_KEY);
		}
		return s3AccessKey;
	}

	@Override
	public String getS3SecretKey() {
		String s3SecretKey = getString(PROP_S3_SECRET_KEY);
		if (s3SecretKey == null || s3SecretKey.isEmpty()) {
			throw new RuntimeException("Invalid property " + PROP_S3_SECRET_KEY);
		}
		return s3SecretKey;
	}

	@Override
	public String getS3Bucket() {
		String s3Bucket = getString(PROP_S3_BUCKET);
		if (s3Bucket == null || s3Bucket.isEmpty()) {
			throw new RuntimeException("Invalid property " + PROP_S3_BUCKET);
		}
		return s3Bucket;
	}

	@Override
	public String getS3Prefix() {
		String s3Prefix = getString(PROP_S3_PREFIX);
		if (s3Prefix == null || s3Prefix.isEmpty()) {
			throw new RuntimeException("Invalid property " + PROP_S3_PREFIX);
		}
		return s3Prefix.replaceAll("/$", "");
	}

	@Override
	public Map<String, Integer> getTopicsAndPartitions() {
		return getConfigMap(PROP_KAFKA_TOPICS);
	}

	@Override
	public int getS3MaxObjectSize() {
		return getInt(PROP_S3_MAX_OBJECT_SIZE, DEFAULT_S3_SIZE);
	}

	@Override
	public int getKafkaMaxMessageSize() {
		return getInt(PROP_KAFKA_MAX_MESSAGE_SIZE, DEFAULT_MSG_SIZE);
	}

	public Map<String, Integer> getTopicSizes() {
		logger.debug("Topic size map {}", getConfigMap(PROP_S3_TOPIC_SIZES)
				.keySet());
		return getConfigMap(PROP_S3_TOPIC_SIZES);
	}
}
