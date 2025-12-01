package dev.ikm.ike.tinkarizer.entity;

import java.util.UUID;

public record NavigableData(
		String childName,
		UUID childId,
		String parentName,
		UUID parentId) { }
