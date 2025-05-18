package org.yald.domain;

public record Alert(String AccountId, String TransactionId) {
    public Alert {
        if (AccountId == null || AccountId.isBlank()) {
            throw new IllegalArgumentException("AccountId cannot be null or blank");
        }
        if (TransactionId == null || TransactionId.isBlank()) {
            throw new IllegalArgumentException("TransactionId cannot be null or blank");
        }
    }
}
