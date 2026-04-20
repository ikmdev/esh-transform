package dev.ikm.ike.esh.tinkarizer.etl.load;

import dev.ikm.tinkar.entity.StampEntity;

public record LoadContext(Writer write, StampEntity<?> activeStampEntity, StampEntity<?> inactiveStampEntity) {

}
