package dev.ikm.ike.esh.tinkarizer.etl.domain;

import java.util.UUID;

import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;

public interface Id {

	public static final UUID ES_NAMESPACE = UUID.fromString("94282870-9279-46c0-b4ae-82cadddf7b7d");
	public static final UUID EC_NAMESPACE = UUID.fromString("7291397d-9ead-4f1e-bcd2-3f8facdcf468");

	default UUID generateEventSetId(String id) {
		return UuidT5Generator.get(ES_NAMESPACE, id);
	}

	default UUID generateEventCodeId(String id) {
		return UuidT5Generator.get(EC_NAMESPACE, id);
	}

	default UUID generateId(UUID namespace, String id) {
		return UuidT5Generator.get(namespace, id);
	}
}
