package de.hpi.ddm.actors;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;

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

		//System.out.println(workerHintMessage.hashedHints[0]);

		// create hint universes (with each missing one character from the characterUniverse)
		List<char[]> hintUniverses = new LinkedList<>();

		for (int i = 0; i < workerHintMessage.passwordLength+1; i++) {
			char[] hintUniverse;
			StringBuilder sb = new StringBuilder();
			sb.append(workerHintMessage.characterUniverse);
			sb.deleteCharAt(i);
			hintUniverse = sb.toString().toCharArray();
			hintUniverses.add(hintUniverse);
		}

		List<String> crackedHints = new LinkedList<>();

		// calculate permutations for each hintUniverse (with 10 characters the amount of permutations is 3628800)
		for (int i = 0; i < hintUniverses.size(); i++) {
			List<String> permutations = new LinkedList<>();
			char[] oneUniverse = hintUniverses.get(i);
			heapPermutation(oneUniverse, oneUniverse.length, oneUniverse.length, permutations);
			//System.out.println(permutations.size());

			// hash all permutations
			for (int j = 0; j < permutations.size(); j++) {
				String hashedPerm;
				hashedPerm = hash(permutations.get(j));
				//System.out.println(hashedPerm);

				// compare hashed permutations with hashed hints
				for (int k = 0; k < workerHintMessage.hashedHints.length; k++) {

					//System.out.println(workerHintMessage.hashedHints[k]);
					//System.out.println(hashedPerm);

					// if equal: send missing character from array in hint
					if(hashedPerm.equals(workerHintMessage.hashedHints[k])) {
						System.out.println(hashedPerm + " " + workerHintMessage.hashedHints[k]);
						crackedHints.add(permutations.get(j));
					}
				}
			}
		}

		for (int i = 0; i < crackedHints.size(); i++) {
			System.out.println(crackedHints.get(i));
		}

		// gefunden --> Hint aus der Hintliste und weiter im Text
		// nicht gefunden --> Buchstabe in passworduniverse einfügen

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
	private void heapPermutation(char[] a, int size, int n, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

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