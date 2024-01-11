package ru.nms.embeddingmasterservice.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.nms.embeddingslibrary.model.Embedding;
import ru.nms.embeddingslibrary.model.WorkerServiceMeta;

@Service
@RequiredArgsConstructor
@Slf4j
public class EmbeddingService {

    private final HashRingService hashRingService;

    private final TransferService transferService;

    public Embedding getEmbeddingById(int id) {
        WorkerServiceMeta workerWithNeededEmbedding = hashRingService.selectWorkerByEmbeddingId(id);
        //todo log.info("Getting embedding with id " + id + " from " + workerWithNeededEmbedding.getName());
        return transferService.getEmbeddingFromWorker(id, HashRingService.hashId(id), workerWithNeededEmbedding.getAddress(), workerWithNeededEmbedding.getPort());
    }
}
