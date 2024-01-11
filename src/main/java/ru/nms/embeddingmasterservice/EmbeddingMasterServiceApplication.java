package ru.nms.embeddingmasterservice;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import ru.nms.embeddingslibrary.model.MasterServiceMeta;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class EmbeddingMasterServiceApplication {

	@Value("${zookeeper.service.base.path}")
	private String basePath;

	@Value("${zookeeper.service.base.max.retry.policy}")
	private int maxRetries;

	@Value("${zookeeper.service.base.sleep.time}")
	private int sleepTimeInMillis;


	@Value("${zookeeper.address}")
	private String zookeeperAddress;

	private CuratorFramework client;


	public static void main(String[] args) {
		SpringApplication.run(EmbeddingMasterServiceApplication.class, args);
	}

    @Bean
	public CuratorFramework createClient() {
		return client;
	}


	private ServiceDiscovery<MasterServiceMeta> serviceDiscovery;
	@Bean
	public ServiceDiscovery<MasterServiceMeta>  getMasterServiceDiscovery() {
		return serviceDiscovery;
	}
	@PostConstruct
	private void registerServerToZookeeper() {
		client = CuratorFrameworkFactory.newClient(zookeeperAddress, new ExponentialBackoffRetry(sleepTimeInMillis, maxRetries));
		client.start();

        JsonInstanceSerializer<MasterServiceMeta> masterSerializer = new JsonInstanceSerializer<>(MasterServiceMeta.class);
		serviceDiscovery = ServiceDiscoveryBuilder.builder(MasterServiceMeta.class)
				.client(client)
				.serializer(masterSerializer)
				.basePath(basePath)
				.watchInstances(true)
				.build();
		try {
			serviceDiscovery.start();
        }
		catch (Exception e) {
			throw new RuntimeException(e);
		}
    }
}
