package ru.nms.embeddingmasterservice.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class EmbeddingNotFoundException extends ResponseStatusException {

    public EmbeddingNotFoundException() {
        super(HttpStatus.NOT_FOUND, "Embedding with given id was not found");
    }


}
