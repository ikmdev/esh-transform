package dev.ikm.ike.esh.tinkarizer.etl.domain;

import java.util.List;
import java.util.UUID;

public record ViewableCanonicalRecord(List<UUID> ids, boolean isActive, String fqn, String syn, String def,
		String identifier, UUID identifierSource) {
}
