package ru.nms.embeddingmasterservice.controller;


import lombok.RequiredArgsConstructor;
import org.apache.curator.x.discovery.ServiceInstance;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import ru.nms.embeddingmasterservice.exception.EmbeddingNotFoundException;
import ru.nms.embeddingmasterservice.service.HashRingService;
import ru.nms.embeddingmasterservice.service.RegisterService;
import ru.nms.embeddingslibrary.model.MasterUpdateDto;
import ru.nms.embeddingslibrary.model.WorkerServiceMeta;

import java.util.Arrays;
import java.util.List;

@RestController
@RequestMapping("/master")
@RequiredArgsConstructor
public class MasterController {

    private final RegisterService registerService;

    private final HashRingService hashRingService;
    @PostMapping("/update")
    public void updateMasterMeta(@RequestBody MasterUpdateDto masterUpdateDto) {
        System.out.println("got post request for updating ");
        registerService.update(masterUpdateDto.getCurrentWorkers(), masterUpdateDto.getRing());
    }

    @GetMapping("/workers/{hash}")
    public WorkerServiceMeta[] getWorkersWithHash(@PathVariable int hash) {
        return hashRingService.getWorkersByHash(hash).toArray(WorkerServiceMeta[]::new);
    }

    @ExceptionHandler({
            EmbeddingNotFoundException.class
    })
    public ResponseEntity<String> handleBadRequestExceptions(
            ResponseStatusException exception
    ) {
        return ResponseEntity
                .status(exception.getStatus())
                .body(exception.getMessage());
    }
}