package kafka.s3.consumer;

import java.io.IOException;
import java.util.Observer;

import kafka.message.Message;
import kafka.message.MessageAndMetadata;

interface Sink {

	public long append(MessageAndMetadata<byte[], byte[]> messageAndOffset)
			throws IOException;
	public void addObserver(Observer o);
}