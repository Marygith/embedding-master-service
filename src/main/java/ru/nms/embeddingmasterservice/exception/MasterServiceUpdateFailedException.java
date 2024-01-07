package ru.nms.embeddingmasterservice.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class MasterServiceUpdateFailedException extends ResponseStatusException {
    public MasterServiceUpdateFailedException(String reason) {
        super(HttpStatus.CONFLICT, reason);
    }
}
