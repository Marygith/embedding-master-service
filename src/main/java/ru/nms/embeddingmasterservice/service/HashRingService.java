package ru.nms.embeddingmasterservice.service;

import lombok.RequiredArgsConstructor;
import org.apache.curator.x.discovery.ServiceInstance;
import org.springframework.stereotype.Service;
import ru.nms.embeddingslibrary.model.VirtualNodeMeta;
import ru.nms.embeddingslibrary.model.WorkerServiceMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import static ru.nms.embeddingmasterservice.util.Constants.HASH_RING_SIZE;
import static ru.nms.embeddingmasterservice.util.Constants.REPLICATION_FACTOR;

@Service
@RequiredArgsConstructor
public class HashRingService {
    private final TreeMap<Integer, VirtualNodeMeta> ring = new TreeMap<>();


    public void initHashRing(List<ServiceInstance<WorkerServiceMeta>> servers) {
        for(int i = 1; i < HASH_RING_SIZE; i++) {
            var virtualNodeMeta = VirtualNodeMeta.builder().
                    owners(new ArrayList<>())
                    .hash(i - 1)
                    .build();

            ring.put(i - 1, virtualNodeMeta);

            for(int k = 0; k < REPLICATION_FACTOR; k++) {
                servers.get((k + i) % servers.size()).getPayload().getVirtualNodeHashes().add(i - 1);
                virtualNodeMeta.getOwners().add(servers.get((k + i) % servers.size()).getPayload());
            }
        }
    }
    //100 virtual nodes(sets of vectors)
    //replication factor 3 -> 300 sets
    //10 servers
    //each server contains 30 vector sets
    //another one comes along
    //it should contain ~ 30 vector sets
    //new server should find 30 server and take one set from them
    public void addNode(WorkerServiceMeta newNode, int aliveNodesAmount) {
        int virtNodeHash = ThreadLocalRandom.current().nextInt(0, HASH_RING_SIZE);
        int hashRingStep = HASH_RING_SIZE / aliveNodesAmount;
        for(int i = 0; i < aliveNodesAmount * REPLICATION_FACTOR; i++) {
            var virtMeta = ring.get(virtNodeHash);

            //remove virtual node replica from first of servers
            WorkerServiceMeta serviceMeta  = virtMeta.getOwners().removeFirst();
            serviceMeta.getVirtualNodeHashes().remove(virtNodeHash);

            //assign new virtual node to new server
            newNode.getVirtualNodeHashes().add(virtNodeHash);
            virtMeta.getOwners().add(newNode);

            //todo actual data transfer

            virtNodeHash += hashRingStep;
        }
    }

    public void deleteNode(WorkerServiceMeta deletedNode, List<WorkerServiceMeta> aliveNodes) {
        int firstRandomServerInd = ThreadLocalRandom.current().nextInt(0, aliveNodes.size());
        for(Integer orphanedVirtNodeHash : deletedNode.getVirtualNodeHashes()) {

            //find orphaned virtual node meta and delete deleted server from it
            var virtMeta = ring.get(orphanedVirtNodeHash);
            virtMeta.getOwners().remove(deletedNode);

            //add virtual node to new server
            var serviceMeta = aliveNodes.get(firstRandomServerInd % aliveNodes.size());
            serviceMeta.getVirtualNodeHashes().add(orphanedVirtNodeHash);
            virtMeta.getOwners().add(serviceMeta);

            //todo actual data transfer

            firstRandomServerInd++;

        }
    }

    public WorkerServiceMeta select(int embeddingId) {
        int embeddingHash = embeddingId % HASH_RING_SIZE;
        List<WorkerServiceMeta> servers = ring.get(embeddingHash).getOwners();
        return servers.get(ThreadLocalRandom.current().nextInt(0, servers.size()));
    }


}
