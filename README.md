# Master service for embeddings retrieval project
## Overview

Master service accepts requests for embeddings retrieval and manages[ worker nodes ](https://github.com/Marygith/embeddings-server) . To provide fault tolerance, there can be several master nodes.

## Embedding retrieval

Receives requests to endpoint **/embeddings/{id}**, hashes id, chooses one of the workers, that has embeddings with id with the same hash.
Forwards request to the chosen worker, waits for the response and returns it.

## Workers' management

Workers are highly loaded nodes, that do all the business logic and store all the data. Due to hgih load ot errors, 
they can easily die, or can be added to scale app horizontally. Master nodes reacts to this changes, reassigning data among the workers evenly.
Also, master maintains fault-tolerance, so each data shard has 3 replicas, and two replicas of the same shard can not be stored on the same worker node.

## Masters' management

If there are several master instances, request to retrieve embeddings can be worwarded to eny of them, as reading does not change data.

Workers' management, though, can be enforced only by one of the masters, that's why leader election implemented. Onle leader instance listens to workers' events.

## Requirements

This project requires java 21 and running zookeeper(by defult address - localhost:2181)
