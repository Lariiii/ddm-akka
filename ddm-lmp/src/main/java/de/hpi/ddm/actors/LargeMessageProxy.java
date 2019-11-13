package de.hpi.ddm.actors;

import java.io.*;

import akka.actor.*;
import com.esotericsoftware.kryo.Kryo;
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
		private int id;
		private ActorRef sender;
		private ActorRef receiver;
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

		// convert serialized byteMessage into chunks with hardcoded size
		// TODO: check if dynamically assigning a chunk size is better
		byte[][] chunks = divideArray(byteMessage, 4096);

		for(int i=0; i < chunks.length; i++){
			BytesMessage<byte[]> msg = new BytesMessage<>();
			msg.bytes = chunks[i];
			// TODO: check whether there is a better way to set the id for reassembling
			msg.id = i;
			//System.out.println(msg.id + " A " + msg.bytes);
			msg.receiver = receiver;
			msg.sender = this.sender();
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
        } catch (Exception e){
            return null;
        }
    }

    // divide the byte message array into chunks
	private static byte[][] divideArray(byte[] byteMessage, int chunksize) {
		// TODO: check if byteMessage is completely divided into chunks
		byte[][] result = new byte[((int)Math.ceil(byteMessage.length)/(int)chunksize)+1][chunksize];
		int start = 0;

		for(int i = 0; i < result.length; i++) {
			if(start + chunksize > byteMessage.length) {
				System.arraycopy(byteMessage, start, result[i], 0, byteMessage.length - start);
			} else {
				System.arraycopy(byteMessage, start, result[i], 0 , chunksize);
			}
			start += chunksize;
		}

		return result;
	}

	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.

		// TODO: reassemble the chunks
		//System.out.println(message.id + " B " + message.bytes);
        message.getReceiver().tell(message.getBytes(), message.getSender());

	}
}
