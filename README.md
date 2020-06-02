# Custom Map Reduce Framework

## Why make a custom Map Reduce framework?

Most map reduce frameworks require hardware nodes to enact the full parrallel processing power of their map reduce framework. While that is ideal for large jobs, what if a map reduce framework was built to support medium sized jobs? That is rather than only using the map reduce framework for massize amounts of files what if a lightweight mapreduce framework was created that could process lots of local files on one machine in the fasted way possible? Typically multi threaded programs are difficult to write,  but this map reduce framework is meant to make the life of a programmer easier by being able to abstract away all multi threaded code from small to medium sized jobs by combining the idea and architecture of the map reduce framework with golangs natural and simple support for multi threaded programming.

## State of project

All of the core logic is implemented and unit tested using multiple different map reduce functions and different datasets. The project can as of now do all that was intended, it can parallel process a large amount of files using map and reduce functions. The code however hasn't been used extensively outside the testing so is in that way unproven. However it has been unit tested for multiple failures across nodes.

No major updates were made as of October 2019.

## What is Map Reduce and How It Was Implemented

The first task in designing and implementing MapReduce was reading the architecture and understanding the system at a conceptual level. At a production level, MapReduce is used to distribute very large batch processing tasks across horizontally scalable hardware. It was developed at Google to quickly process lots of web crawled data to index it for their search engine.

The abstraction for the system is Map and Reduce jobs. By implementing data processing into Map and Reduce functional primitives, the MapReduce framework will distribute the processing by first applying a Map job on key value pairs to generate a set of intermediary keys and then by applying a reduce function on all those intermediary keys. Although not mentioned in the original paper, MapReduce can be extended to MapReduceMerge, which allows for more complex relational database-like joins on previously mapped or reduced jobs. This wasn’t part of the original lab, but I considered implementing it as an extension to the system, but unfortunately there wasn’t enough time.
Underlying the abstraction is a system design where the data is partitioned across several worker nodes by a master node. Map and Reduce tasks are assigned to these worker nodes. The worker nodes then process the keys and store the intermediary values in a local file system. Then the master node assigns the reduce tasks to idle worker who then process the reduce tasks and write them to output files to the local file system.
I was tasked to build a system that mirrors this in Golang. The system begins with a user being able to write two functions, Map and Reduce. The system communicates with each other via remote procedure calls where the Master first assigns workers to map tasks. Once all the map tasks are complete, it assigns them to reduce tasks. This is a deviation from the original implementation, as map and reduce tasks are both assigned immediately by the master node, and once a map task is finished a reduce task is taken to that node immediately. This leads to suboptimal performance in my implementation, but wasn’t realized until the system design was near complete.
The master node keeps track of worker state and assign them tasks accordingly. Fault tolerance was implemented by checking for incomplete jobs, and if found the master node will reassign a non finished job to a different worker. This is simplistic to the original implementation, which typically needs to set up a ping system similar to what was done for the key value system I built.
