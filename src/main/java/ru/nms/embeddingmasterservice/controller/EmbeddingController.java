package ru.nms.embeddingmasterservice.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import ru.nms.embeddingmasterservice.exception.EmbeddingNotFoundException;
import ru.nms.embeddingmasterservice.service.EmbeddingService;
import ru.nms.embeddingslibrary.model.Embedding;

@RestController
@RequestMapping("/embeddings")
@RequiredArgsConstructor
@Slf4j
public class EmbeddingController {

    private final EmbeddingService embeddingService;

    @GetMapping("/{id}")
    public Embedding getEmbeddingById(@PathVariable int id) {
//        log.info("getting embedding " + id);
        return embeddingService.getEmbeddingById(id);
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
