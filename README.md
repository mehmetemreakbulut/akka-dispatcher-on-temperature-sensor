## Description of message flows

We have 3 actor types; DispatcherActor, SensorProcessorActor, TemperatureSensorActor. 

We also added a new message type that isConfigMsg.

DispatcherActor:
We have two HashMap for Load Balancer, SensorCountsForLB and SensorToProcessorMap.

SensorCountsForLB maps the processors to number of sensor they have assigned and SensorToProcessorMap handles the dispatcher act to which processor it should send the msg. 
For round robin we have one list(SensorToProcessorList) and one index that handles where are we inside our list. According to DispatchLogic Msg, we are changing the behavior of the actor. When it is RoundRobin and when a temperature message is received, we are incrementing currentProcessor to move forward in the list. In LoadBalancer,when a temperature message is received, the LoadBalancer checks if the corresponding sensor is already mapped to a processor. If it is, the message is forwarded to that processor. If not, it identifies the processor with the minimum message count, incrementally assigns the sensor to that processor, updates the message count, and then forwards the message to the chosen processor. 
when creating sesnors we send a ConfigMsg so they know the dispatcher. 

In the main, we crated sensors and dispatcher, also we use GenerateMsg type to tell the sensors that they .Sensors tells to dispatcher temperature info with TemperatureMsg. We also changed to RR and Loadbalancer and visaversa. 

For error handling we throw an exception in the Processor, the handler and master is the dispatcher (which has created them) it handles it with OneToOne and resumes after a failure.
