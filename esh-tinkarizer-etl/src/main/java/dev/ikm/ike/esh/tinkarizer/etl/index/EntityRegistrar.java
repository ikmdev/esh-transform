package dev.ikm.ike.esh.tinkarizer.etl.index;

import java.util.UUID;

public interface EntityRegistrar {

	void register(UUID namespace, String identifier);

}
