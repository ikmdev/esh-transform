package dev.ikm.ike.tinkarizer;

import dev.ikm.tinkar.terms.EntityProxy;

import java.util.UUID;

record ViewableData(
		UUID namespace,
		String id,
		boolean isActive,
		String fqn,
		String syn,
		String def,
		String identifier) { }
