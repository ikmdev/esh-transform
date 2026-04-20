package dev.ikm.ike.esh.tinkarizer.etl.load;

import java.util.UUID;

import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.terms.EntityProxy.Concept;

public record LoadConfig(long time, UUID author, UUID module, UUID path) {
	public Concept authorConcept() {
		PublicId authorPublicId = PublicIds.of(author);
		return Concept.make(authorPublicId);
	}

	public PublicId authorPublicId() {
		return PublicIds.of(author);
	}

	public Concept moduleConcept() {
		PublicId modulePublicId = PublicIds.of(module);
		return Concept.make(modulePublicId);
	}

	public PublicId modulePublicId() {
		return PublicIds.of(module);
	}

	public Concept pathConcept() {
		PublicId pathPublicId = PublicIds.of(path);
		return Concept.make(pathPublicId);
	}

	public PublicId pathPublicId() {
		return PublicIds.of(path);
	}

}
