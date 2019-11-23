package de.hpi.ddm.actors;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import scala.Char;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	HashMap<Character, HashSet> allPermutations = new HashMap<>();
	// for testing
	private int workerNumber;

    /////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(Master.WorkerHintMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(Master.WorkerHintMessage workerHintMessage) {
		List<Character> crackedCharacters = new LinkedList<>();
		// workerHintMessage.hintUniverses.forEach((key,value) -> System.out.println(key + " = " + String.valueOf((char[]) value)));

		// generate all permutations for all possible hints (once)
		if (allPermutations.isEmpty()) {
			workerNumber = workerHintMessage.id;
			System.out.println("GO! Worker " + workerNumber + " started permutating!");
			Iterator<Map.Entry<Character, char[]>> it = workerHintMessage.hintUniverses.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<Character, char[]> pair = it.next();
				HashSet<String> permutationSet = new HashSet<>();
				heapPermutation(pair.getValue(), pair.getValue().length, pair.getValue().length, permutationSet);
				allPermutations.put(pair.getKey(), permutationSet);
				// for (String permutation : permutationSet) { System.out.println(permutation); }
			}
			System.out.println("DONE! Worker " + workerNumber + " done permutating!");
		}

		// crack hints
		System.out.println("START! Worker " + workerNumber + " cracks hints!");
		System.out.println("WORKER " + workerNumber + " allPermutations.size(): " + allPermutations.size());
		Iterator<Map.Entry<Character, HashSet>> it = allPermutations.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Character, HashSet> pair = it.next();
			// System.out.println("WORKER " + workerNumber + " hintKey: " + pair.getKey());
			for(Object hint : workerHintMessage.hashedHints) {
				// System.out.println("WORKER " + workerNumber + " bin in innerer for-Schleife");
				if (pair.getValue().contains(hint)) {
					System.out.println("HINT 1 of WORKER " + workerNumber + " with key: " + pair.getKey());
					crackedCharacters.add(pair.getKey());
				}
			}
		}
		char[] crackedCharactersArray = new char[crackedCharacters.size()];
		for (int i = 0; i < crackedCharacters.size(); i++) {
			crackedCharactersArray[i] = crackedCharacters.get(i);
		}

		// TODO: Why is crackedCharactersArray empty?

		// generate permutations for the password
		System.out.println("START! Worker " + workerNumber + " creates passwordpermutations!");
		System.out.println("crackedCharactersArray.length: " + crackedCharactersArray.length + "workerHintMessage.passwordLength: " + workerHintMessage.passwordLength);
		HashSet<String> passwordPermutations = new HashSet<>();
		heapPermutation(crackedCharactersArray, workerHintMessage.passwordLength, workerHintMessage.passwordLength, passwordPermutations);
		System.out.println("DONE! Worker " + workerNumber + " created passwordpermutations!");

		// crack password
		for(String permutation : passwordPermutations) {
			if (workerHintMessage.hashedPassword.equals(permutation)) {
				System.out.println("YEAH, Worker " + workerNumber + " cracked password " + workerHintMessage.id);
			}
		}

		//TODO: send hint message containing the two characters for the password permutation back to master
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
	private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, int n, HashSet<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(hash(new String(a)));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, n, l);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}
}