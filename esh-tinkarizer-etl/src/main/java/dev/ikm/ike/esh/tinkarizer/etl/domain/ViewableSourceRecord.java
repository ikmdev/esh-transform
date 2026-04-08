package dev.ikm.ike.esh.tinkarizer.etl.domain;

import java.util.UUID;

public record ViewableSourceRecord(UUID namespace, String id, String status, String fqn, String syn, String def) {
}
