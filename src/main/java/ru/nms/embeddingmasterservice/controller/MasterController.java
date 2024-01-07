package ru.nms.embeddingmasterservice.controller;


import lombok.RequiredArgsConstructor;
import org.apache.curator.x.discovery.ServiceInstance;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.nms.embeddingmasterservice.service.RegisterService;
import ru.nms.embeddingslibrary.model.WorkerServiceMeta;

import java.util.Arrays;
import java.util.List;

@RestController
@RequestMapping("/master")
@RequiredArgsConstructor
public class MasterController {

    private final RegisterService registerService;
    @PostMapping("/update")
    public void updateMasterMeta(@RequestBody List<ServiceInstance<WorkerServiceMeta>> currentWorkers) {
        System.out.println("got post request for updating " + Arrays.toString(currentWorkers.toArray()));
        registerService.update(currentWorkers);
    }
}
