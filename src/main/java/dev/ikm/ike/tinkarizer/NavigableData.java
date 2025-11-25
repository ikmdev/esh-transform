package dev.ikm.ike.tinkarizer;

import java.util.UUID;

public record NavigableData(
		UUID childNamespace,
		String childId,
		UUID parentNamespace,
		String parentId) { }
