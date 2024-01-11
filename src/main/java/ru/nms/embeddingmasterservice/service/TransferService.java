package ru.nms.embeddingmasterservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.server.ResponseStatusException;
import ru.nms.embeddingmasterservice.exception.EmbeddingNotFoundException;
import ru.nms.embeddingslibrary.model.Embedding;
import ru.nms.embeddingslibrary.model.TransferRequest;

import javax.annotation.PostConstruct;

@Service
@Slf4j
public class TransferService {

    private final RestTemplate restTemplate = new RestTemplate();

    private final HttpHeaders headers = new HttpHeaders();

    @PostConstruct
    private void init() {
        headers.setContentType(MediaType.APPLICATION_JSON);

    }

    public void transferEmbeddings(int embeddingsHash, String destAddress, int destPort, String sourceAddress, int sourcePort) {
        HttpEntity<TransferRequest> requestEntity = new HttpEntity<>(TransferRequest.builder().hash(embeddingsHash).address(destAddress).port(destPort).build(), headers);
        String url = createUrl(sourceAddress, sourcePort, "/worker/transfer");

        try {
            restTemplate.postForObject(url, requestEntity, ResponseEntity.class);
        } catch (HttpStatusCodeException exception) {
            if(exception.getStatusCode().equals(HttpStatus.NOT_FOUND)) {
                //todo log.warn("Source worker with port " + sourcePort + " does not have this embedding");
            }
        }

    }

    public Embedding getEmbeddingFromWorker(int embeddingId, int hash, String workerAddress, int workerPort) {
        String url = createUrl(workerAddress, workerPort, "/worker/embeddings/" + hash + "/" + embeddingId);
        //todo log.info("Url for getting embedding: " + url);
        ResponseEntity<Embedding> response = null;
        try {
            response = restTemplate.getForEntity(url, Embedding.class);
        } catch (HttpClientErrorException exception) {
            if(exception.getStatusCode().equals(HttpStatus.NOT_FOUND)) {
                //todo log.warn("Embeddingg with id " + embeddingId + " was not found");
               throw new EmbeddingNotFoundException();
            }
            throw new RuntimeException(exception);
        }
        return response.getBody();
    }

    private String createUrl(String address, int port, String endpoint) {
        return "http://" + address + ":" + port + endpoint;
    }

}
