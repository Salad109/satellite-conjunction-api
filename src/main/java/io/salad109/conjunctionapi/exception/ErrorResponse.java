package io.salad109.conjunctionapi.exception;

public record ErrorResponse(
        int status,
        String error,
        String details,
        String timestamp
) {
}
