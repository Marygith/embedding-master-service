package ru.nms.embeddingmasterservice.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.x.discovery.*;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import ru.nms.embeddingmasterservice.exception.MasterServiceUpdateFailedException;
import ru.nms.embeddingslibrary.model.MasterServiceMeta;
import ru.nms.embeddingslibrary.model.WorkerServiceMeta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Service
@RequiredArgsConstructor
public class MasterService {

    private final CuratorFramework client;

    private final RegisterService registerService;

    private final ServiceDiscovery<MasterServiceMeta> masterServiceDiscovery;

    @Value("${zookeeper.service.base.path}")
    private String basePath;

    @Value("${zookeeper.worker.service.name}")
    private String workerServiceName;


    @Value("${zookeeper.master.service.name}")
    private String masterServiceName;

    private ServiceCache<WorkerServiceMeta> workerServiceCache;

    private ServiceDiscovery<WorkerServiceMeta> serviceDiscovery;

    private ServiceProvider<WorkerServiceMeta> workerServiceProvider;

    private RestTemplate restTemplate;

    private HttpHeaders headers;

    @PostConstruct
    private void init() {
        restTemplate = new RestTemplate();
        headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
    }

    public void initWorkerInstancesListener() {

        JsonInstanceSerializer<WorkerServiceMeta> serializer = new JsonInstanceSerializer<>(WorkerServiceMeta.class);

        List<ServiceInstance<WorkerServiceMeta>> currentInstances;


        serviceDiscovery = ServiceDiscoveryBuilder.builder(WorkerServiceMeta.class)
                .client(client)
                .serializer(serializer)
                .basePath(basePath)
                .watchInstances(true)
                .build();

        workerServiceProvider = serviceDiscovery.serviceProviderBuilder().serviceName(workerServiceName)
                .build();


        try {
            currentInstances = registerService.getInstance().getPayload().getCurrentWorkers();
        } catch (Exception e) {
            throw new RuntimeException();
        }


        workerServiceCache = serviceDiscovery.serviceCacheBuilder()
                .name(workerServiceName)
                .build();


        workerServiceProvider = serviceDiscovery.serviceProviderBuilder().serviceName(workerServiceName).build();

        workerServiceCache.addListener(new ServiceCacheListener() {
            @Override
            public void cacheChanged() {

                List<ServiceInstance<WorkerServiceMeta>> instanceList = null;
                try {
                    instanceList = new ArrayList<>(workerServiceProvider.getAllInstances());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                Iterator<ServiceInstance<WorkerServiceMeta>> providerIt = instanceList.iterator();

                while (instanceList.size() > currentInstances.size() && providerIt.hasNext()) {
                    ServiceInstance<WorkerServiceMeta> instance = providerIt.next();
                    if (!currentInstances.contains(instance)) {
                        System.out.println(instance.getPayload().getName() + " has been added" + "\n");
                        currentInstances.add(instance);
                    }
                }

                Iterator<ServiceInstance<WorkerServiceMeta>> currentIt = currentInstances.iterator();
                while (currentIt.hasNext()) {
                    ServiceInstance<WorkerServiceMeta> instance = currentIt.next();
                    if (!instanceList.contains(instance)) {
                        System.out.println(instance.getPayload().getName() + " has been removed" + "\n");
                        currentIt.remove();
                    }
                }

                update(currentInstances);

            }

            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState state) {
                System.out.println("STATE CHANGED");
            }
        });


        try {
            workerServiceProvider.start();
            serviceDiscovery.start();
            workerServiceCache.start();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void close() {
        try {
            serviceDiscovery.close();
            workerServiceCache.close();
            workerServiceCache.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void update(List<ServiceInstance<WorkerServiceMeta>> newWorkers) {

        try {

            for (ServiceInstance<MasterServiceMeta> instance : masterServiceDiscovery.queryForInstances(masterServiceName)) {
                if (instance.getId().equals(registerService.getInstance().getId())) {
                    instance.getPayload().setCurrentWorkers(newWorkers);
                    masterServiceDiscovery.updateService(instance);
                    continue;
                }
                System.out.println("\nupdate service " + instance.getPayload().getName() + ", setting workers from " + Arrays.toString(instance.getPayload().getCurrentWorkers().stream().map(i -> i.getPayload().getName()).toArray()) + " to " + Arrays.toString(newWorkers.stream().map(i -> i.getPayload().getName()).toArray()));
                instance.getPayload().setCurrentWorkers(newWorkers);

                String url = "http://" + instance.getAddress() + ":" + instance.getPort() + "/master/update";

                HttpEntity<List<ServiceInstance<WorkerServiceMeta>>> requestEntity = new HttpEntity<>(instance.getPayload().getCurrentWorkers(), headers);

                try {
                    restTemplate.postForObject(url, requestEntity, ResponseEntity.class);
                } catch (RestClientException e) {
                    throw new MasterServiceUpdateFailedException(e.getMessage());
                }

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
