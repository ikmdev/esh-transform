package dev.ikm.ike.esh.tinkarizer.etl.loader;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.composer.assembler.ConceptAssembler;
import dev.ikm.tinkar.composer.template.Definition;
import dev.ikm.tinkar.composer.template.FullyQualifiedName;
import dev.ikm.tinkar.composer.template.Identifier;
import dev.ikm.tinkar.composer.template.StatedAxiom;
import dev.ikm.tinkar.composer.template.Synonym;
import dev.ikm.tinkar.composer.template.USDialect;
import dev.ikm.tinkar.terms.EntityProxy.Concept;
import dev.ikm.tinkar.terms.EntityProxy.Semantic;
import dev.ikm.tinkar.terms.TinkarTermV2;

public class SimpleLoader extends AbstractLoader {

	private final static Logger LOG = org.slf4j.LoggerFactory.getLogger(SimpleLoader.class);

	public SimpleLoader(String composerName, long time, UUID author, UUID module, UUID path) {
		super(composerName, time, author, module, path);
	}

	@Override
	public void loadNavigableData(List<NavigableCanonicalRecord> navigableData) {
		navigableData.forEach(data -> {
			Concept reference = Concept.make(PublicIds.of(data.childId()));
			List<Concept> parentConcepts = data.parentIds().stream().map(PublicIds::of).map(Concept::make).toList();
			activeSession.compose(new StatedAxiom().isA(parentConcepts.toArray(new Concept[0])), reference);
		});
		LOG.info("Finish transforming {} Navigable Data", navigableData.size());
	}

	@Override
	public void loadViewableData(List<ViewableCanonicalRecord> viewableData) {
		viewableData.forEach(data -> {
			Concept concept = Concept.make(PublicIds.of(data.conceptId()));

			// Create Concept Active or Inactive
			if (data.isActive()) {
				activeSession
						.compose((ConceptAssembler conceptAssembler) -> conceptAssembler.publicId(concept.publicId()));
				conceptCounter.incrementAndGet();
			} else {
				inactiveSession
						.compose((ConceptAssembler conceptAssembler) -> conceptAssembler.publicId(concept.publicId()));
				conceptCounter.incrementAndGet();
			}

			// Create FQN Semantic
			Semantic fqn = Semantic.make(PublicIds.newRandom());
			activeSession.compose(new FullyQualifiedName().semantic(fqn).language(TinkarTermV2.ENGLISH_LANGUAGE)
					.text(data.fqn()).caseSignificance(TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE), concept);
			semanticCounter.incrementAndGet();
			activeSession.compose(new USDialect().acceptability(TinkarTermV2.PREFERRED), fqn);
			semanticCounter.incrementAndGet();

			// Create SYN Semantic
			Semantic syn = Semantic.make(PublicIds.newRandom());
			activeSession.compose(new Synonym().semantic(syn).language(TinkarTermV2.ENGLISH_LANGUAGE)
					.text(data.syn().isEmpty() ? data.fqn() : data.syn())
					.caseSignificance(TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE), concept);
			semanticCounter.incrementAndGet();
			activeSession.compose(new USDialect().acceptability(TinkarTermV2.PREFERRED), syn);
			semanticCounter.incrementAndGet();

			// Create DEF Semantic
			Semantic def = Semantic.make(PublicIds.newRandom());
			activeSession.compose(new Definition().semantic(def).language(TinkarTermV2.ENGLISH_LANGUAGE)
					.text(data.def().isEmpty() ? data.fqn() : data.def())
					.caseSignificance(TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE), concept);
			semanticCounter.incrementAndGet();
			activeSession.compose(new USDialect().acceptability(TinkarTermV2.PREFERRED), def);
			semanticCounter.incrementAndGet();

			// Create Identifier Semantic
			if (!data.identifier().isEmpty() && data.identifierSource() != null) {
				Concept identifierSource = Concept.make(PublicIds.of(data.identifierSource()));
				activeSession.compose(new Identifier().source(identifierSource).identifier(data.identifier()),
						concept);
				semanticCounter.incrementAndGet();
			}
		});
		LOG.info("Finish transforming {} Viewable Data", viewableData.size());
	}

}
