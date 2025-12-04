package dev.ikm.ike.tinkarizer.entity;

import java.util.List;
import java.util.UUID;

public record ViewableDatum(
		List<UUID> ids,
		boolean isActive,
		String fqn,
		String syn,
		String def,
		String identifier) { }
