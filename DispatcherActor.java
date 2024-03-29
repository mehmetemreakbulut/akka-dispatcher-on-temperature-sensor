package com.lab.evaluation23;

import akka.actor.*;
import akka.japi.pf.DeciderBuilder;
import com.evaluation22.WorkerActor;

import java.time.Duration;
import java.util.*;

public class DispatcherActor extends AbstractActorWithStash {

	private final static int NO_PROCESSORS = 2;


	//These two maps are used for Load Balancer, maps processors to number of sensors assigned to them
	private final Map<ActorRef, Integer> SensorCountsForLB= new HashMap<ActorRef, Integer>();
	// maps sensors to processors
	private final Map<ActorRef, ActorRef> SensorToProcessorMap = new HashMap<ActorRef, ActorRef>();

	//This list is used for Round Robin, and the currentProcessor is the index of the list
	private final List<ActorRef> SensorToProcessorList = new ArrayList<ActorRef>();
	private int currentProcessor = 0;

	//supervisor strategy for processor
	private static SupervisorStrategy strategy =
			new OneForOneStrategy(
					1, // Max no of retries
					Duration.ofMinutes(1), // Within what time period
					DeciderBuilder.match(Exception.class, e -> SupervisorStrategy.resume())
							.build());

	@Override
	public SupervisorStrategy supervisorStrategy() {
		return strategy;
	}

	public DispatcherActor() {
		for (int i = 0; i < NO_PROCESSORS; i++) {
			ActorRef processor = getContext().actorOf(SensorProcessorActor.props(), "p" + i);
			SensorToProcessorList.add(processor);
			SensorCountsForLB.put(processor, 0);
		}

	}


	@Override
	public AbstractActor.Receive createReceive() {
		return useLoadBalance();
	}

	void changeDispatch(DispatchLogicMsg msg) {
		int method = msg.getLogic();
		if (method == DispatchLogicMsg.ROUND_ROBIN) {
			System.out.println("DISPATCHER: Changing to Round Robin");
			getContext().become(useRoundRobin());
		} else {
			System.out.println("DISPATCHER: Changing to Load Balancer");
			getContext().become(useLoadBalance());
		}
	}


	private final AbstractActor.Receive useLoadBalance(){
		return receiveBuilder()
			.match(TemperatureMsg.class, this::dispatchDataLoadBalancer)
				.match(DispatchLogicMsg.class, this::changeDispatch)
			.build();
	}

	private final AbstractActor.Receive useRoundRobin(){
		return receiveBuilder()
			.match(TemperatureMsg.class, this::dispatchDataRoundRobin)
				.match(DispatchLogicMsg.class, this::changeDispatch)
			.build();
	}

	private void dispatchDataLoadBalancer(TemperatureMsg msg) {
		System.out.println("DISPATCHER: using Load Balancer");
		ActorRef sensor = msg.getSender();
		if(SensorToProcessorMap.containsKey(sensor)){
			System.out.println("DISPATCHER: Sensor already mapped");
			ActorRef processor = SensorToProcessorMap.get(sensor);
			processor.tell(msg, msg.getSender());
		} else {
			System.out.println("DISPATCHER: Sensor not mapped");
			ActorRef minimumCountP = null;
			int minCount = Integer.MAX_VALUE;
			for(ActorRef a : SensorCountsForLB.keySet()){
				int count = SensorCountsForLB.get(a);
				if(minCount > count) {
					minimumCountP = a;
					minCount = count;
				}
			}
			if (minimumCountP == null) {
				throw new RuntimeException("No processor found");
			} else {
				SensorCountsForLB.put(minimumCountP, minCount+1);
				SensorToProcessorMap.put(sensor, minimumCountP);
				System.out.println(Arrays.asList(SensorToProcessorMap));
				minimumCountP.tell(msg, msg.getSender());
			}

		}

	}

	private void dispatchDataRoundRobin(TemperatureMsg msg) {
		System.out.println("DISPATCHER: using Round Robin");
		System.out.println("DISPATCHER: Current processor is " + currentProcessor);
		ActorRef processor = SensorToProcessorList.get(currentProcessor);
		processor.tell(msg, msg.getSender());
		currentProcessor = (currentProcessor + 1) % NO_PROCESSORS;

	}


	static Props props() {
		return Props.create(DispatcherActor.class);
	}
}
