package dev.ikm.ike.tinkarizer.entity;

public record ViewableExtract(
		Type type,
		String id,
		String status,
		String fqn,
		String syn,
		String def) { }
