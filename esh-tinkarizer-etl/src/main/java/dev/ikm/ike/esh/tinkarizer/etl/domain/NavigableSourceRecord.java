package dev.ikm.ike.esh.tinkarizer.etl.domain;

import java.util.UUID;

public record NavigableSourceRecord(UUID namespace, String childId, String parentId) {
}
