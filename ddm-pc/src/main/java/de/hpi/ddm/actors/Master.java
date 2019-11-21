package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
	}

	public static Queue<WorkerHintMessage> hintMessageQueue;

	////////////////////
	// Actor Messages //
	////////////////////

	public static class WorkerHintMessage<T> implements Serializable {
		private static final long serialVersionUID = 8107711559395710783L;
		int id;
		T[] hashedHints;
		int passwordLength;
		String characterUniverse;
		String result;
	}

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;

	private long startTime;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void handle(BatchMessage message) {
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////

		// der reader schickt schon lauter chunks als message an den Master, wodurch diese Methode gerufen wird

		// Maybe only use first worker and first line of data

		// Master creates Array with possible chars and int with length of pw (new object)
		// Master sends object and hint to crack
		// Worker cracks hint:
			// Worker creates heap permutation
			// Worker hashes permutation
			// Worker compares hash and hint
			// if equal: send missing character from array in hint

		// goal: have one worker crack one line of data (batches in workers.size()?)

		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
		}

		hintMessageQueue = new LinkedList<>();
		WorkerHintMessage<String> request = new WorkerHintMessage<>();
        int nextWorker = 0;

		for (String[] line : message.getLines()){
			//WorkerHintMessage<String> request = new WorkerHintMessage<>();
			request.id = Integer.parseInt(line[0]);
			request.characterUniverse = line[2];
			request.passwordLength = Integer.parseInt(line[3]);
			request.hashedHints = Arrays.copyOfRange(line, 5, line.length);

			//TODO: send the WorkerHintMessages from the queue to idle workers
			hintMessageQueue.add(request);

            workers.get(nextWorker).tell(request, this.self());
            nextWorker = ((nextWorker + 1) % workers.size());
            System.out.println(nextWorker);
		}

		//TODO: check whether this distributes to all workers

		/*for (ActorRef worker : this.workers) {
			//System.out.println(worker);
			worker.tell(request, this.self());
		}*/

		//this.workers.get(0).tell(request, this.self());

		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
//		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
	}
}
