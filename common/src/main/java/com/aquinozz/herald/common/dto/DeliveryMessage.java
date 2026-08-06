package com.aquinozz.herald.common.dto;

/**
 * Mensagem que representa uma entrega individual (1 evento -> 1 endpoint).
 * Trafega entre webhook-dispatcher e retry-consumer nos topicos delivery/retry/dlq.
 *
 * @param eventId     id do evento original
 * @param appId       id do app
 * @param endpointId  id do endpoint destino
 * @param endpointUrl url do endpoint destino
 * @param type        tipo do evento
 * @param payload     corpo original do evento (JSON em string), que sera assinado e enviado
 * @param attempt     numero da tentativa atual (comeca em 1)
 * @param maxAttempts limite de tentativas antes de ir para a DLQ
 * @param nextDelayMs delay (ms) que o retry-consumer deve esperar antes da proxima tentativa
 */
public record DeliveryMessage(
        String eventId,
        Long appId,
        Long endpointId,
        String endpointUrl,
        String type,
        String payload,
        int attempt,
        int maxAttempts,
        long nextDelayMs) {
}