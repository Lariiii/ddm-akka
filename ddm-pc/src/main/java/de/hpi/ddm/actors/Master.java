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
		this.allPasswords = new HashMap<>();
	}

	public static Queue<WorkerCrackRequest> hintMessageQueue;

	////////////////////
	// Actor Messages //
	////////////////////

	public static class WorkerCrackRequest<T> implements Serializable {
		private static final long serialVersionUID = 8107711559395710783L;
		public final int id;
		public final T[] hashedHints;
		public final int passwordLength;
		public final String hashedPassword;
		public final ActorRef replyTo;
		public final char[] characterUniverse;

		public WorkerCrackRequest(int id, T[] hashedHints, int passwordLength, String hashedPassword, ActorRef replyTo, char[] characterUniverse){
			this.id = id;
			this.hashedHints = hashedHints;
			this.passwordLength = passwordLength;
			this.hashedPassword = hashedPassword;
			this.replyTo = replyTo;
			this.characterUniverse = characterUniverse;
		}
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
	}

	public static class MasterResponse<T1, T2> implements Serializable {
		private static final long serialVersionUID = 7480612328579267137L;
		public final T1 left;
		public final T2 right;

		public MasterResponse(T1 left, T2 right) {
			this.left = left;
			this.right = right;
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
	private HashMap<Integer, String> allPasswords = new HashMap<>();
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
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();

		this.reader.tell(new Reader.ReadMessage(), this.self());
	}

	protected void requestHintPermutations(String characterUniverse){
		HashMap<Character, char[]> hintUniverses = new HashMap<>();

		for (int i = 0; i < characterUniverse.length(); i++) {
			StringBuilder sb = new StringBuilder();
			sb.append(characterUniverse);
			char hintKey = sb.charAt(i);
			sb.deleteCharAt(i);
			char[] hintUniverse = sb.toString().toCharArray();
			hintUniverses.put(hintKey, hintUniverse);
		}

		int nextWorker = 0;
		ArrayList<Future<Object>> futureList = new ArrayList<>();
		Timeout timeout = new Timeout(5, TimeUnit.MINUTES);
		HashMap<Character, HashSet> allPermutations = new HashMap<>();

		for (int i = 0; i < characterUniverse.length(); i++) {
			Character character = characterUniverse.charAt(i);
			ActorRef worker = workers.get(nextWorker);
			Future<Object> futureOut = Patterns.ask(
					worker,
					new HintPermutationRequest(nextWorker+1, character, hintUniverses.get(character), this.self()),
					timeout
			);
			futureList.add(futureOut);
			workingWorkers.add(worker);

			// All workers are busy? Collect results before proceeding.
			if (workingWorkers.size() == workers.size()) {
				for (Future<Object> futureIn : futureList) {
					try {
						MasterResponse<Character, HashSet> output = (MasterResponse) Await.result(futureIn, timeout.duration());
						allPermutations.put(output.left, output.right);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				workingWorkers.clear();
			}
			nextWorker = ((nextWorker + 1) % workers.size());
		}

		for (Future<Object> futureIn : futureList) {
			try {
				MasterResponse<Character, HashSet> output = (MasterResponse) Await.result(futureIn, timeout.duration());
				allPermutations.put(output.left, output.right);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		workingWorkers.clear();

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

		int nextWorker = 0;
		ArrayList<Future<Object>> futureList = new ArrayList<>();

		if (!permutationsDone) {
			String characterUniverse = message.getLines().get(0)[2];
			requestHintPermutations(characterUniverse);
		}

		// create hint character universes
		// get character set and password length from first line
		for (String[] line: message.getLines()) {
			int id = Integer.parseInt(line[0]);
			int passwordLength = Integer.parseInt(line[3]);
			String hashedPassword = line[4];
			String[] hashedHints = Arrays.copyOfRange(line, 5, line.length);
			Timeout timeout = new Timeout(1, TimeUnit.MINUTES);
			ActorRef worker = workers.get(nextWorker);
			Future<Object> futureOut = Patterns.ask(
					worker,
					new WorkerCrackRequest<String>(id, hashedHints, passwordLength, hashedPassword, this.self(), line[2].toCharArray()),
					timeout
			);
			futureList.add(futureOut);
			workingWorkers.add(worker);

			if (workingWorkers.size() == workers.size()) {
				for (Future<Object> futureIn : futureList) {
					try {
						MasterResponse<Integer, String> output = (MasterResponse) Await.result(futureIn, timeout.duration());
						allPasswords.put(output.left, output.right);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				workingWorkers.clear();
			}
			nextWorker = ((nextWorker + 1) % workers.size());
		}

		for (Map.Entry<Integer, String> pair : allPasswords.entrySet()) {
			System.out.println("id: " + pair.getKey() + " password: " + pair.getValue());
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
