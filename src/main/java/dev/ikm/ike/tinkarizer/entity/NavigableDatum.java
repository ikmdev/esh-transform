package dev.ikm.ike.tinkarizer.entity;

import java.util.List;
import java.util.UUID;

public record NavigableDatum(
		UUID childId,
		List<UUID> parentIds) { }
