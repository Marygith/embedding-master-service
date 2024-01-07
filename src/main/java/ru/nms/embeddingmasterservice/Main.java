package ru.nms.embeddingmasterservice;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import ru.nms.embeddingslibrary.model.MasterServiceMeta;
import ru.nms.embeddingslibrary.model.WorkerServiceMeta;

import java.util.Arrays;

public class Main {


    public static void main(String[] args) {
        JsonInstanceSerializer<MasterServiceMeta> masterSerializer = new JsonInstanceSerializer<MasterServiceMeta>(MasterServiceMeta.class);
        JsonInstanceSerializer<WorkerServiceMeta> workerSerializer = new JsonInstanceSerializer<WorkerServiceMeta>(WorkerServiceMeta.class);

        String basePath = "/main_service";
        String zookeeperAddress = "127.0.0.1:2181";
        try (

                CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress, new ExponentialBackoffRetry(1000, 3));

                ServiceDiscovery<MasterServiceMeta> masterServiceDiscovery = ServiceDiscoveryBuilder.builder(MasterServiceMeta.class)
                        .client(client)
                        .serializer(masterSerializer)
                        .basePath(basePath)
                        .watchInstances(true)
                        .build();

                ServiceDiscovery<WorkerServiceMeta> workerServiceDiscovery = ServiceDiscoveryBuilder.builder(WorkerServiceMeta.class)
                        .client(client)
                        .serializer(workerSerializer)
                        .basePath(basePath)
                        .watchInstances(true)
                        .build();

        ) {
            client.start();
            masterServiceDiscovery.start();
            workerServiceDiscovery.start();

            String workerServiceName = "worker-service";
            System.out.println("trying to update " + Arrays.toString(workerServiceDiscovery.queryForInstances(workerServiceName).toArray()));

            String masterServiceName = "master-service";
            System.out.println("trying to update " + Arrays.toString(masterServiceDiscovery.queryForInstances(masterServiceName).toArray()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
