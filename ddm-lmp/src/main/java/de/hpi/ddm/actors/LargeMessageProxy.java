package de.hpi.ddm.actors;

import java.io.*;
import java.util.ArrayList;
import java.util.*;

import akka.actor.*;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import scala.util.control.Exception;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	private ArrayList<Byte> messageList = new ArrayList<Byte>() ;

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
		private byte[] bytes;
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

		int i = 0;
		for(; i < chunks.length; i++){
			BytesMessage msg = new BytesMessage();
			msg.bytes = chunks[i];
			// TODO: check whether there is a better way to set the id for reassembling
			msg.id = i;
			System.out.println(msg.id + " A " + msg.bytes);
			msg.receiver = receiver;
			msg.sender = this.sender();
			receiverProxy.tell(msg, this.self());
		}
		BytesMessage closingMessage = new BytesMessage();
		closingMessage.bytes = convertToBytes("close");
		closingMessage.id =  i + 1;
		closingMessage.receiver = receiver;
		closingMessage.sender = this.sender();
		receiverProxy.tell(closingMessage, this.self());
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

	private void handle(BytesMessage message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.

		// TODO: reassemble the chunks
		System.out.println(message.id + " B " + message.bytes);

		if (message.bytes == convertToBytes("close")) {
			byte[] completeArray = new byte[messageList.size()];
			for (int i = 0; i <= messageList.size(); i++)
				completeArray[i] = messageList.get(i);
			// completeArray = messageList.toArray(completeArray);

			Object object = null;
			Kryo kryo = new Kryo();
			ByteArrayInputStream stream = new ByteArrayInputStream(completeArray);
			Input input = new Input(stream);
			object = kryo.readClassAndObject(input);
			input.close();

			message.getReceiver().tell(object, message.getSender());
		} else {
			for (byte b : message.bytes) {
				messageList.add(b);
			}
		}


	}
}
