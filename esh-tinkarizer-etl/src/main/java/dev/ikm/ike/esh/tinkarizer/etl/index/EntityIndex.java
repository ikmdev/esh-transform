package dev.ikm.ike.esh.tinkarizer.etl.index;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class EntityIndex implements EntityLookup, EntityRegistrar {

	private final Set<IndexKey> entries = new HashSet<>();

	@Override
	public boolean exists(UUID namespace, String identifier) {
		return entries.contains(new IndexKey(namespace, identifier));
	}

	@Override
	public void register(UUID namespace, String identifier) {
		entries.add(new IndexKey(namespace, identifier));
	}

}
