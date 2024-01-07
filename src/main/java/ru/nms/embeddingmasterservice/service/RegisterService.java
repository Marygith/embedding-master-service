package ru.nms.embeddingmasterservice.service;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.nms.embeddingmasterservice.exception.MasterServiceUpdateFailedException;
import ru.nms.embeddingslibrary.model.MasterServiceMeta;
import ru.nms.embeddingslibrary.model.WorkerServiceMeta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
@RequiredArgsConstructor
public class RegisterService {

    @Value("${zookeeper.master.service.name}")
    private String masterServiceName;

    @Value("${zookeeper.master.service.instance.name}")
    private String instanceName;
    @Value("${server.port}")
    private int port;

    @Value("${zookeeper.master.id}")
    private String id;

    private final CuratorFramework client;

    private final ServiceDiscovery<MasterServiceMeta> masterServiceDiscovery;

    @Getter
    private ServiceInstance<MasterServiceMeta> instance;

    @PostConstruct
    private void initInstance() {
        try {
            masterServiceDiscovery.start();

            instance = ServiceInstance.<MasterServiceMeta>builder()
                    .id(id)
                    .name(masterServiceName)
                    .port(port)
                    .address("localhost")   //If address is not written, you will take your local IP.
                    .payload(new MasterServiceMeta(id, instanceName, new ArrayList<>()))
                    .build();

            masterServiceDiscovery.registerService(instance);


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void update(List<ServiceInstance<WorkerServiceMeta>> currentWorkers) {
        System.out.println("\ncame new workers " + Arrays.toString(currentWorkers.stream().map(i -> i.getPayload().getName()).toArray()) + " instead of old ones " + Arrays.toString(instance.getPayload().getCurrentWorkers().stream().map(i -> i.getPayload().getName()).toArray()));
        instance.getPayload().setCurrentWorkers(currentWorkers);
        try {
            masterServiceDiscovery.updateService(instance);
        } catch (Exception e) {
            throw new MasterServiceUpdateFailedException(e.getMessage());
        }
    }

}
