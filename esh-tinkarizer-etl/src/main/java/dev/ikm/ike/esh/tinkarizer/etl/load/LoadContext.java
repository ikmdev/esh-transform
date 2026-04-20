package dev.ikm.ike.esh.tinkarizer.etl.load;

import dev.ikm.tinkar.entity.StampEntity;
import dev.ikm.tinkar.entity.transaction.Transaction;

public record LoadContext(Transaction transaction, StampEntity<?> activeStampEntity,
		StampEntity<?> inactiveStampEntity, Write write) {

}
