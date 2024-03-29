package com.lab.evaluation23;

import akka.actor.AbstractActor;
import akka.actor.Props;

public class SensorProcessorActor extends AbstractActor {

	private double currentAverage;
	private int count;

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(TemperatureMsg.class, this::gotData).build();
	}

	private void gotData(TemperatureMsg msg) throws Exception {

		System.out.println("SENSOR PROCESSOR " + self() + ": Got data from " + msg.getSender());

		//throw exception when temp < 0, excluding from average computation
		if(msg.getTemperature() < 0) {
			throw new Exception(self() + " Temperature is below 0");
		} else {
			currentAverage = (currentAverage * count + msg.getTemperature()) / (count + 1);
			System.out.println(count + "  "+ self());
			count++;
			System.out.println("SENSOR PROCESSOR " + self() + ": Current avg is " + currentAverage);
		}
		

	}

	static Props props() {
		return Props.create(SensorProcessorActor.class);
	}

	public SensorProcessorActor() {
		this.currentAverage = 0;
		this.count = 0;
	}
}
