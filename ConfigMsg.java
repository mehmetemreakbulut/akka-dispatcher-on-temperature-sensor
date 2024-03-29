package com.lab.evaluation23;

import akka.actor.ActorRef;

public class ConfigMsg {

    private ActorRef dispatcher;

    public ConfigMsg (ActorRef broker) {
        this.dispatcher = broker;
    }

    public ActorRef getBrokerRef() {
        return dispatcher;
    }
}
