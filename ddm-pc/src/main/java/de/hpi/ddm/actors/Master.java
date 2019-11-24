package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import akka.pattern.Patterns;
import akka.util.Timeout;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import scala.concurrent.Await;
import scala.concurrent.Future;

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
		this.workingWorkers = new ArrayList<>();
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
		String hashedPassword;
		HashMap<Character, char[]> hintUniverses;
	}

	public static class HintPermutationRequest implements Serializable {
		private static final long serialVersionUID = -1427710472671723834L;
		//public final ActorRef master;
		public final int id;
		public final Character hintCharacter;
		public final char[] hintUniverse;
		public final ActorRef replyTo;

		public HintPermutationRequest(
				int id,
				Character hintCharacter,
				char[] hintUniverse,
				ActorRef replyTo
		) {
			this.id = id;
			this.hintCharacter = hintCharacter;
			this.hintUniverse = hintUniverse;
			this.replyTo = replyTo;
		}
		// Character hintCharacter;
		// char[] hintUniverse;
	}

	// could probably also just be implemented as a general Response class with different contents
	public static class HintPermutationResponse implements Serializable {
		private static final long serialVersionUID = 7480612328579267137L;
		public final Character hintCharacter;
		public final HashSet<String> permutationSet;

		public HintPermutationResponse(Character hintCharacter, HashSet<String> permutationSet) {
			this.hintCharacter = hintCharacter;
			this.permutationSet = permutationSet;
		}
	}

	public static class PermutationsMessage implements Serializable {
		private static final long serialVersionUID = -112664771927463149L;
		public final HashMap<Character, HashSet> allPermutations;

		public PermutationsMessage(HashMap<Character, HashSet> allPermutations) {
			this.allPermutations = allPermutations;
		}
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
	private final List<ActorRef> workingWorkers;
	private boolean permutationsDone = false;

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
				//.match(HintPermutationResponse.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();

		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	/*protected void handle(HintPermutationResponse message) {
		System.out.println("REMOVING A WORKER, workingWorkers.size(): " + workingWorkers.size());
		workingWorkers.remove(message.worker);
		System.out.println("REMOVED A WORKER, workingWorkers.size(): " + workingWorkers.size());
	}*/

	protected void requestHintPermutations(String characterUniverse){
		HashMap<Character, char[]> hintUniverses = new HashMap<>();

		for (int i = 0; i < characterUniverse.length(); i++) {
			StringBuilder sb = new StringBuilder();
			sb.append(characterUniverse);
			char hintKey = sb.charAt(i);
			sb.deleteCharAt(i);
			char[] hintUniverse = sb.toString().toCharArray();
			hintUniverses.put(hintKey, hintUniverse);
			System.out.println(hintUniverse);
		}

		int nextWorker = 0;
		ArrayList<Future<Object>> futureList = new ArrayList<>();
		Timeout timeout = new Timeout(5, TimeUnit.MINUTES);

		System.out.println("MASTER sending tasks to workers");
		for (Map.Entry<Character, char[]> hint : hintUniverses.entrySet()) {
			Future<Object> future = Patterns.ask(
					workers.get(nextWorker),
					new HintPermutationRequest(nextWorker+1, hint.getKey(), hint.getValue(), this.self()),
					timeout);
			futureList.add(future);
			//nextWorker = ((nextWorker + 1) % workers.size());
		}

		/*
		for (int i = 0; i < workers.size(); i++) {
			ActorRef worker = workers.get(i);

			Future<Object> future = Patterns.ask(
					worker,
					new HintPermutationRequest(i+1, hintUniverses, this.self()),
					timeout);
			futureList.add(future);
			// workingWorkers.add(worker);
			nextWorker = ((nextWorker + 1) % workers.size());
		}
		 */

		HashMap<Character, HashSet> allPermutations = new HashMap<>();

		System.out.println("MASTER collecting results from workers");
		for (Future<Object> future : futureList) {
			try {
				HintPermutationResponse output = (HintPermutationResponse) Await.result(future, timeout.duration());
				allPermutations.put(output.hintCharacter, output.permutationSet);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		for (ActorRef worker : workers) {
			worker.tell(new PermutationsMessage(allPermutations), this.self());
		}

		permutationsDone = true;
	}

	protected void handle(BatchMessage message) {

		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////

		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
			return;
		}

		HashMap<Character, char[]> hintUniverses = new HashMap<>();
		int nextWorker = 0;

		if (!permutationsDone) {
			String characterUniverse = message.getLines().get(0)[2];
			requestHintPermutations(characterUniverse);
		}

		// create hint character universes
		// get character set and password length from first line
		for (String[] line: message.getLines()) {
			WorkerHintMessage<String> request = new WorkerHintMessage<>();
			request.id = Integer.parseInt(line[0]);
			request.hintUniverses = hintUniverses;
			request.passwordLength = Integer.parseInt(line[3]);
			request.hashedPassword = line[4];
			request.hashedHints = Arrays.copyOfRange(line, 5, line.length);

			workers.get(nextWorker).tell(request, this.self());
			nextWorker = ((nextWorker + 1) % workers.size());
		}

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
