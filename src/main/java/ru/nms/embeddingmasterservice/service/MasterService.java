package ru.nms.embeddingmasterservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
import ru.nms.embeddingslibrary.model.MasterUpdateDto;
import ru.nms.embeddingslibrary.model.VirtualNodeMeta;
import ru.nms.embeddingslibrary.model.WorkerServiceMeta;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class MasterService {

    private final CuratorFramework client;

    private final RegisterService registerService;

    private final ServiceDiscovery<MasterServiceMeta> masterServiceDiscovery;

    private final HashRingService hashRingService;

    @Value("${zookeeper.service.base.path}")
    private String basePath;

    @Value("${zookeeper.worker.service.name}")
    private String workerServiceName;


    @Value("${zookeeper.master.service.name}")
    private String masterServiceName;

    private ServiceCache<WorkerServiceMeta> workerServiceCache;

    private ServiceDiscovery<WorkerServiceMeta> workerServiceDiscovery;

    private ServiceProvider<WorkerServiceMeta> workerServiceProvider;

    private RestTemplate restTemplate;

    private HttpHeaders headers;

    @PostConstruct
    private void init() {
        restTemplate = new RestTemplate();
        headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        JsonInstanceSerializer<WorkerServiceMeta> serializer = new JsonInstanceSerializer<>(WorkerServiceMeta.class);
        List<ServiceInstance<WorkerServiceMeta>> workers = new ArrayList<>();
        try {

            TreeMap<Integer, VirtualNodeMeta> ring;
//            if (masterServiceDiscovery.queryForInstances(masterServiceName).size() == 1) {
            workerServiceDiscovery = ServiceDiscoveryBuilder.builder(WorkerServiceMeta.class)
                    .client(client)
                    .serializer(serializer)
                    .basePath(basePath)
                    .watchInstances(true)
                    .build();

            workerServiceProvider = workerServiceDiscovery.serviceProviderBuilder().serviceName(workerServiceName)
                    .build();

            workerServiceDiscovery.start();
            workerServiceProvider.start();
            //todo log.info("Initializing first master, " + Arrays.toString(workerServiceProvider.getAllInstances().toArray()) + " found");
            workers.addAll(workerServiceProvider.getAllInstances());
            workerServiceProvider.close();
            workerServiceDiscovery.close();
//            }
            ring = hashRingService.initHashRing(workers);

            if (masterServiceDiscovery.queryForInstances(masterServiceName).size() > 1) {
                MasterServiceMeta aliveMaster = masterServiceDiscovery.queryForInstances(masterServiceName).stream().filter(master -> !Objects.equals(master.getId(), registerService.getInstance().getId())).findAny().get().getPayload();
                hashRingService.setRing(aliveMaster.getRing());
            }

            updateSelf(workers, ring);
         /*   if (!workers.isEmpty()) {
                update(workers, ring);
            }*/

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

/*        JsonInstanceSerializer<WorkerServiceMeta> serializer = new JsonInstanceSerializer<>(WorkerServiceMeta.class);

        serviceDiscovery = ServiceDiscoveryBuilder.builder(WorkerServiceMeta.class)
                .client(client)
                .serializer(serializer)
                .basePath(basePath)
                .watchInstances(true)
                .build();

        workerServiceProvider = serviceDiscovery.serviceProviderBuilder().serviceName(workerServiceName)
                .build();


        try {
            workerServiceProvider.start();
            serviceDiscovery.start();
            workerServiceCache.start();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }*/
    }

    public void initWorkerInstancesListener() {

        JsonInstanceSerializer<WorkerServiceMeta> serializer = new JsonInstanceSerializer<>(WorkerServiceMeta.class);

        List<ServiceInstance<WorkerServiceMeta>> currentInstances;

        workerServiceDiscovery = ServiceDiscoveryBuilder.builder(WorkerServiceMeta.class)
                .client(client)
                .serializer(serializer)
                .basePath(basePath)
                .watchInstances(true)
                .build();

        workerServiceProvider = workerServiceDiscovery.serviceProviderBuilder().serviceName(workerServiceName)
                .build();


        try {
            currentInstances = registerService.getInstance().getPayload().getCurrentWorkers();
        } catch (Exception e) {
            throw new RuntimeException();
        }

        workerServiceCache = workerServiceDiscovery.serviceCacheBuilder()
                .name(workerServiceName)
                .build();

        workerServiceCache.addListener(new ServiceCacheListener() {
            @Override
            public void cacheChanged() {

                List<ServiceInstance<WorkerServiceMeta>> instanceList = null;
                if (registerService.getInstance().getPayload().getRing().get(0) != null) {
                    hashRingService.setRing(registerService.getInstance().getPayload().getRing());
                }

                try {
                    instanceList = new ArrayList<>(workerServiceProvider.getAllInstances());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                TreeMap<Integer, VirtualNodeMeta> ring = new TreeMap<>();
                //todo log.info("Workers from provider " + Arrays.toString(instanceList.stream().map(s -> s.getPayload().getName()).toArray()));
                //todo log.info("Workers from this instance " + Arrays.toString(currentInstances.stream().map(s -> s.getPayload().getName()).toArray()));
                if (instanceList.size() == currentInstances.size()) return;
                Iterator<ServiceInstance<WorkerServiceMeta>> providerIt = instanceList.iterator();

                while (instanceList.size() > currentInstances.size() && providerIt.hasNext()) {
                    ServiceInstance<WorkerServiceMeta> instance = providerIt.next();
                    if (currentInstances.stream().map(ServiceInstance::getId).noneMatch(id -> Objects.equals(id, instance.getId()))) {
                        System.out.println(instance.getPayload().getName() + " has been added" + "\n");
                        ring = hashRingService.addNode(instance.getPayload(), Math.max(1, currentInstances.size()));
                        currentInstances.add(instance);
                    }
                }

                Iterator<ServiceInstance<WorkerServiceMeta>> currentIt = currentInstances.iterator();
                while (currentIt.hasNext()) {
                    ServiceInstance<WorkerServiceMeta> instance = currentIt.next();
                    if (instanceList.stream().map(ServiceInstance::getId).noneMatch(id -> Objects.equals(id, instance.getId()))) {
                        System.out.println(instance.getPayload().getName() + " has been removed" + "\n");
                        ring = hashRingService.deleteNode(instance.getPayload(), instanceList.stream().map(ServiceInstance::getPayload).toList());
                        currentIt.remove();
                    }
                }

                update(currentInstances, ring);

            }

            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState state) {
                System.out.println("STATE CHANGED");
            }
        });

        try {
            workerServiceProvider.start();
            workerServiceDiscovery.start();
            workerServiceCache.start();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void close() {
        try {
            workerServiceDiscovery.close();
            workerServiceCache.close();
            workerServiceCache.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void update(List<ServiceInstance<WorkerServiceMeta>> newWorkers, TreeMap<Integer, VirtualNodeMeta> ring) {
        MasterUpdateDto dto = new MasterUpdateDto(newWorkers, ring);
        try {

            for (ServiceInstance<MasterServiceMeta> instance : masterServiceDiscovery.queryForInstances(masterServiceName)) {
                if (instance.getId().equals(registerService.getInstance().getId())) {
                    updateSelf(newWorkers, ring);
                    continue;
                }
                System.out.println("\nupdate service " + instance.getPayload().getName() + ", setting workers from " + Arrays.toString(instance.getPayload().getCurrentWorkers().stream().map(i -> i.getPayload().getName()).toArray()) + " to " + Arrays.toString(newWorkers.stream().map(i -> i.getPayload().getName()).toArray()));
                instance.getPayload().setCurrentWorkers(newWorkers);
                instance.getPayload().setRing(ring);

                String url = "http://" + instance.getAddress() + ":" + instance.getPort() + "/master/update";

                HttpEntity<MasterUpdateDto> requestEntity = new HttpEntity<>(dto, headers);

                try {
                    restTemplate.postForEntity(url, requestEntity, MasterUpdateDto.class);
                } catch (RestClientException e) {
                    throw new MasterServiceUpdateFailedException(e.getMessage());
                }

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void updateSelf(List<ServiceInstance<WorkerServiceMeta>> newWorkers, TreeMap<Integer, VirtualNodeMeta> ring) {
        MasterUpdateDto dto = new MasterUpdateDto(newWorkers, ring);
        try {
            registerService.getInstance().getPayload().setCurrentWorkers(newWorkers);
            registerService.getInstance().getPayload().setRing(ring);
            registerService.getInstance().getPayload().setRing(ring);
            masterServiceDiscovery.updateService(registerService.getInstance());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
