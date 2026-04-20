package dev.ikm.ike.esh.tinkarizer.etl.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public record NavigableCanonicalRecord(UUID namespace, boolean isActive, UUID childId, List<UUID> parentIds) {

	public NavigableCanonicalRecord with(UUID parentId) {
		List<UUID> newParentIds = new ArrayList<>(parentIds);
		newParentIds.add(parentId);
		return new NavigableCanonicalRecord(namespace, isActive, childId, newParentIds);	
	}
}
