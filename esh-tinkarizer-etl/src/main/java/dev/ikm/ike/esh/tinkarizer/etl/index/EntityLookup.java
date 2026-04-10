package dev.ikm.ike.esh.tinkarizer.etl.index;

import java.util.UUID;

public interface EntityLookup {
	boolean exists(UUID namespace, String identifier);
}
