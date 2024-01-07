package ru.nms.embeddingmasterservice.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class LeaderService extends LeaderSelectorListenerAdapter{

    private final CuratorFramework client;

    private  LeaderSelector leaderSelector;


    private final MasterService masterService;

    @Value("${zookeeper.master.service.instance.name}")
    private String name;

    @Value("${zookeeper.service.base.path}")
    private String basePath;


    @PostConstruct
    private void start() {
        leaderSelector = new LeaderSelector(client, basePath + "/" + "leader", this);
        leaderSelector.autoRequeue();
        leaderSelector.start();
        System.out.println("Hello, my name is " + name);
    }


    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
        final int waitSeconds = (int) (5 * Math.random()) + 15;
        System.out.println(name + " is now the leader. Waiting " + waitSeconds + " seconds...");

        masterService.initWorkerInstancesListener();

              try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
        } catch (InterruptedException e) {
            System.err.println(name + " was interrupted.");
            Thread.currentThread().interrupt();
        } finally {

            System.out.println(name + " relinquishing leadership.\n");
            masterService.close();
        }
    }
}
