package ru.nms.embeddingmasterservice.service;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.springframework.stereotype.Service;
import ru.nms.embeddingmasterservice.exception.EmbeddingNotFoundException;
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
@Slf4j
public class HashRingService {

    @Setter
    private TreeMap<Integer, VirtualNodeMeta> ring = new TreeMap<>();

    private final TransferService transferService;

    public TreeMap<Integer, VirtualNodeMeta> initHashRing(List<ServiceInstance<WorkerServiceMeta>> servers) {
        //todo log.info("initializing hash ring with " + servers.size() + " workers");
        for (int i = 0; i < HASH_RING_SIZE; i++) {
            var virtualNodeMeta = VirtualNodeMeta.builder()
                    .owners(new ArrayList<>())
                    .hash(i)
                    .build();

            ring.put(i, virtualNodeMeta);

            for (int k = 0; k < Math.min(REPLICATION_FACTOR, servers.size()); k++) {
                servers.get((k + i) % servers.size()).getPayload().getVirtualNodeHashes().add(i);
                virtualNodeMeta.getOwners().add(servers.get((k + i) % servers.size()).getPayload());
            }
        }
        return ring;
    }

    //100 virtual nodes(sets of vectors)
    //replication factor 3 -> 300 sets
    //10 servers
    //each server contains 30 vector sets
    //another one comes along
    //it should contain ~ 30 vector sets
    //new server should find 30 server and take one set from them

    //100 virtual nodes
    //1 server, replication factor = 1
    //eac
    public TreeMap<Integer, VirtualNodeMeta>  addNode(WorkerServiceMeta newNode, int aliveNodesAmount) {
        //todo log.info("Adding new node - " + newNode.getName());
        if (aliveNodesAmount < REPLICATION_FACTOR) {
            //todo log.info("Amount of workers is less, than replication factor(" + REPLICATION_FACTOR + "), so adding all virtual nodes to new worker");
            for (int i = 0; i < HASH_RING_SIZE; i++) {
                var virtualNodeMeta = ring.get(i);
                if(!virtualNodeMeta.getOwners().isEmpty()) {
                    transferService.transferEmbeddings(i, newNode.getAddress(), newNode.getPort(), virtualNodeMeta.getOwners().getFirst().getAddress(), virtualNodeMeta.getOwners().getFirst().getPort());

                }
                newNode.getVirtualNodeHashes().add(i);
                virtualNodeMeta.getOwners().add(newNode);
            }
            return ring;
        }
        int amountOfVirtualReplicas = REPLICATION_FACTOR * HASH_RING_SIZE;
        int amountOfReplicasForNewNode = amountOfVirtualReplicas / aliveNodesAmount;
        //todo log.info("\nAdding " + amountOfReplicasForNewNode + " virtual nodes to new worker");
        int virtNodeHash = ThreadLocalRandom.current().nextInt(0, HASH_RING_SIZE);
        int hashRingStep = HASH_RING_SIZE / amountOfReplicasForNewNode;
        for (int i = 0; i < amountOfReplicasForNewNode; i++) {
            var virtMeta = ring.get(virtNodeHash);
            //todo log.info("Adding virtual node with hash " + virtNodeHash);

            //remove virtual node replica from random owner
            WorkerServiceMeta sourceWorker = virtMeta.getOwners().get(ThreadLocalRandom.current().nextInt(0, virtMeta.getOwners().size()));
            //todo log.info("As source worker was chosen " + sourceWorker.getName());
            sourceWorker.getVirtualNodeHashes().remove(Integer.valueOf(virtNodeHash));

            //assign new virtual node to new server
            newNode.getVirtualNodeHashes().add(virtNodeHash);
            virtMeta.getOwners().add(newNode);

            //actual data transfer
            transferService.transferEmbeddings(virtNodeHash, newNode.getAddress(), newNode.getPort(), sourceWorker.getAddress(), sourceWorker.getPort());
            //todo log.info("transfer from " + sourceWorker.getName() + " to " + newNode.getName() + " is completed\n");
            virtNodeHash = (virtNodeHash + hashRingStep) % HASH_RING_SIZE;
        }
        return ring;

    }

    public TreeMap<Integer, VirtualNodeMeta>  deleteNode(WorkerServiceMeta deletedNode, List<WorkerServiceMeta> aliveNodes) {
        if(aliveNodes.isEmpty()) {
            ring.forEach((key, value) -> value.getOwners().clear());
            return ring;
        }
        //todo log.info("\nDeleting node " + deletedNode.getName());
        int firstRandomServerInd = ThreadLocalRandom.current().nextInt(0, aliveNodes.size());
        //todo log.info("Deleted node had " + deletedNode.getVirtualNodeHashes().size() + " virtual nodes");
        for (Integer orphanedVirtNodeHash : deletedNode.getVirtualNodeHashes()) {
            //todo log.info("Orphaned virtual node with hash " + orphanedVirtNodeHash);
            //find orphaned virtual node meta and delete deleted server from it
            var virtMeta = ring.get(orphanedVirtNodeHash);
            var nodeToDelete = virtMeta.getOwners().stream().filter(w -> w.getPort() != deletedNode.getPort()).findAny().get();
            //todo log.info("Deleting " + nodeToDelete + " from owners of orphaned virt node");
            virtMeta.getOwners().remove(nodeToDelete);

            //find alive worker with orphaned virtual node
            var sourceWorker = virtMeta.getOwners().get(ThreadLocalRandom.current().nextInt(0, virtMeta.getOwners().size()));
            //todo log.info("As source worker was chosen " + sourceWorker.getName());

            //find alive worker available for receiving virtual data
            WorkerServiceMeta destWorker;
            do {
                destWorker = aliveNodes.get(firstRandomServerInd % aliveNodes.size());
                firstRandomServerInd++;
            }
            while (destWorker.getPort() == sourceWorker.getPort());

            //todo log.info("As destination worker for orphaned virtual node was chosen " + destWorker.getName());


            destWorker.getVirtualNodeHashes().add(orphanedVirtNodeHash);
            virtMeta.getOwners().add(destWorker);

            //actual data transfer
            transferService.transferEmbeddings(orphanedVirtNodeHash, destWorker.getAddress(), destWorker.getPort(), sourceWorker.getAddress(), sourceWorker.getPort());
            //todo log.info("transfer from " + sourceWorker.getName() + " to " + destWorker.getName() + " is completed\n");


        }
        return ring;


    }

    public List<WorkerServiceMeta> getWorkersByHash(int hash) {
        //todo log.info("Found " + ring.get(hash).getOwners().size() + " workers with hash " + hash);
        return ring.get(hash).getOwners();
    }

    public WorkerServiceMeta selectWorkerByEmbeddingId(int embeddingId) {
        int embeddingHash = hashId(embeddingId);
        List<WorkerServiceMeta> servers = ring.get(embeddingHash).getOwners();
        if(servers.isEmpty()) throw new EmbeddingNotFoundException();
        return servers.get(ThreadLocalRandom.current().nextInt(0, servers.size()));
    }


    public static int hashId(int id) {
        return id % HASH_RING_SIZE;
    }

}
