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

	// to test whether we get the same byte array after reassembling
	private byte[] end;

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
	public static class BytesMessage implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		// TODO: change byte[] into T again
		private byte[] bytes;
		private int length;
		private ActorRef sender;
		private ActorRef receiver;
		private byte[] original;
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
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming.
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...

		// serialize message into byte array using kryo
		byte[] byteMessage = convertToBytes(message.getMessage());

		//System.out.println(byteMessage.length);

		// convert serialized byteMessage into chunks with hardcoded size
		int chunksize = 4096;
		byte[][] chunks = divideArray(byteMessage, chunksize);

		int i = 0;
		for(; i < chunks.length; i++){
			BytesMessage msg = new BytesMessage();
			msg.bytes = chunks[i];
			msg.receiver = receiver;
			msg.sender = this.sender();
			msg.length = byteMessage.length;
			msg.original = byteMessage;
			receiverProxy.tell(msg, this.self());
		}
	}

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

	private void handle(BytesMessage message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		for (int i=0; i< message.bytes.length; i++){
			messageList.add(message.bytes[i]);
		}

		//System.out.println(messageList.size());

		if (messageList.size() == message.length) {
			byte[] result = messageList.stream()
					.collect(
							() -> new ByteArrayOutputStream(),
							(b, e) -> {
								b.write(e);
							},
							(a, b) -> {}).toByteArray();

			// TODO: check whether this kryo stuff is needed
			/*
			Object object = null;
			Kryo kryo = new Kryo();
			ByteArrayInputStream stream = new ByteArrayInputStream(result);
			Input input = new Input(stream);
			object = kryo.readClassAndObject(input);
			input.close();
			*/

			// check whether the original message matches the resulting message
			end = result;
			System.out.println(Arrays.equals(message.original,end));

			message.getReceiver().tell(result, message.getSender());
		}
	}
}
