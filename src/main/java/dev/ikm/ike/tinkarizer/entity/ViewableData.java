package dev.ikm.ike.tinkarizer.entity;

import java.util.UUID;

public record ViewableData(
		UUID id,
		boolean isActive,
		String fqn,
		String syn,
		String def,
		String identifier) { }
