package dev.ikm.ike.esh.tinkarizer.etl.impl;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import dev.ikm.ike.esh.tinkarizer.etl.Loader;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.composer.Composer;
import dev.ikm.tinkar.composer.Session;
import dev.ikm.tinkar.terms.EntityProxy.Concept;
import dev.ikm.tinkar.terms.State;

public abstract class AbstractLoader implements Loader, AutoCloseable {

	protected final Composer composer;
	protected final Session activeSession;
	protected final Session inactiveSession;

	protected final AtomicInteger conceptCounter;
	protected final AtomicInteger semanticCounter;

	public AbstractLoader(String composerName, long time, UUID author, UUID module, UUID path) {
		this.composer = new Composer(composerName);
		Concept authorConcept = Concept.make(PublicIds.of(author));
		Concept moduleConcept = Concept.make(PublicIds.of(module));
		Concept pathConcept = Concept.make(PublicIds.of(path));
		this.activeSession = composer.open(State.ACTIVE,time, authorConcept, moduleConcept, pathConcept);
		this.inactiveSession = composer.open(State.INACTIVE,time, authorConcept, moduleConcept, pathConcept);
		this.conceptCounter = new AtomicInteger(0);
		this.semanticCounter = new AtomicInteger(0);
	}

	@Override
	public void close() {
		composer.commitAllSessions();
	}

}
