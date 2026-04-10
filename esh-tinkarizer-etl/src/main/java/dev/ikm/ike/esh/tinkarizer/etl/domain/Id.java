package dev.ikm.ike.esh.tinkarizer.etl.domain;

import java.util.UUID;

import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;

public interface Id {

	static UUID generateId(UUID namespace, String id) {
		return UuidT5Generator.get(namespace, id);
	}
}
