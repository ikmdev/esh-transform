package dev.ikm.ike.esh.tinkarizer.etl.domain;

import java.util.UUID;

public record ViewableCanonicalRecord(UUID namespace, UUID conceptId, boolean isActive, String fqn, String syn, String def,
		String identifier, UUID identifierSource) {
}
