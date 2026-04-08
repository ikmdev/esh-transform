package dev.ikm.ike.esh.tinkarizer.etl.domain;

import java.util.List;
import java.util.UUID;

public record NavigableCanonicalRecord(UUID childId, List<UUID> parentIds) {
}
