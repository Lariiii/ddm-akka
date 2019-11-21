package de.hpi.ddm.actors;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

import akka.actor.*;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";

	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	private ArrayList<Byte> messageList = new ArrayList<>();
	private byte[][] chunks;
	private byte[] byteMessage;
	private int iterator = 0;

	////////////////////
	// Actor Messages //
	////////////////////

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
		private Integer origMessageLength;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class ChunkedLargeMessage implements Serializable {
		private static final long serialVersionUID = 1556023214536514407L;
		private ActorRef sender;
		private ActorRef receiver;
		private Integer origMessageLength;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class ChunkedSendMessage<T> implements Serializable {
		private static final long serialVersionUID = -7209931488302530105L;
		private ActorRef sender;
		private ActorRef receiver;
		private Integer origMessageLength;
		private Integer iter;
	}

	/////////////////
	// Actor State //
	/////////////////

	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.match(ChunkedLargeMessage.class, this::handle)
				.match(ChunkedSendMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		byteMessage = convertToBytes(message.getMessage());

		int chunksize = 4096;
		assert byteMessage != null;

		chunks = divideArray(byteMessage, chunksize);

		// send chunked messages
		ChunkedLargeMessage aa = new ChunkedLargeMessage();
		aa.sender = this.sender();
		aa.receiver = receiver;
		aa.origMessageLength = byteMessage.length;

		receiverProxy.tell(aa, this.self());

	}

	// serialize message into byte array using kryo
	// kryo documentation: https://github.com/EsotericSoftware/kryo
	private byte[] convertToBytes(Object message) {
		try {
			Kryo kryo = new Kryo();
			ByteArrayOutputStream stream = new ByteArrayOutputStream();
			Output output = new Output(stream);
			kryo.writeClassAndObject(output, message);
			output.close();
			stream.close();
			return stream.toByteArray();
		} catch (Throwable e){
			return null;
		}
	}

	// divide the byte message array into chunks
	private byte[][] divideArray(byte[] byteMessage, int chunkSize){
		int lastChunk = byteMessage.length % chunkSize;
		int chunks = byteMessage.length / chunkSize + (lastChunk > 0 ? 1 : 0);
		byte[][] arrays = new byte[chunks][];
		if (lastChunk > 0) {
			for (int i = 0; i < chunks - 1; i++) {
				arrays[i] = Arrays.copyOfRange(byteMessage, i * chunkSize, i * chunkSize + chunkSize);
			}
			arrays[chunks - 1] = Arrays.copyOfRange(byteMessage, (chunks - 1) * chunkSize, (chunks - 1) * chunkSize + lastChunk);
		} else {
			for (int i = 0; i < chunks; i++) {
				arrays[i] = Arrays.copyOfRange(byteMessage, i * chunkSize, i * chunkSize + chunkSize);
			}
		}

		return arrays;
	}

	private void handle(ChunkedLargeMessage message) {
		ChunkedSendMessage send = new ChunkedSendMessage();
		send.sender = message.sender;
		send.receiver = message.receiver;
		send.origMessageLength = message.origMessageLength;
		send.iter = iterator;
		this.sender().tell(send, this.self());
	}

	// use global iterator to iterate through chunks and send each chunk individually
	private void handle(ChunkedSendMessage message) {
		BytesMessage msg = new BytesMessage();
		msg.bytes = chunks[message.iter];
		msg.receiver = message.receiver;
		msg.sender = message.sender;
		msg.origMessageLength = message.origMessageLength;
		this.sender().tell(msg, this.self());
	}

	// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
	private void handle(BytesMessage<?> message) {
		// reassemble chunks into one list
		byte[] byteMessage1 = ((byte[]) message.bytes);
		for (int i = 0; i < ((byte[]) message.bytes).length; i++){
			messageList.add(byteMessage1[i]);
		}

		// convert message list to bytes after the last chunk is send
		if (messageList.size() == message.origMessageLength) {
			byte[] result = messageList.stream()
					.collect(
							ByteArrayOutputStream::new,
							ByteArrayOutputStream::write,
							(a, b) -> {}).toByteArray();

			// deserialize byte message
			Object object;
			Kryo kryo = new Kryo();
			ByteArrayInputStream stream = new ByteArrayInputStream(result);
			Input input = new Input(stream);
			object = kryo.readClassAndObject(input);
			input.close();

			message.getReceiver().tell(object, message.getSender());
		} else {
			// increase iterator to send next chunk
			iterator += 1;

			ChunkedSendMessage next = new ChunkedSendMessage();
			next.sender = message.sender;
			next.receiver = message.receiver;
			next.origMessageLength = message.origMessageLength;
			next.iter = iterator;

			this.sender().tell(next, this.self());
		}
	}
}
