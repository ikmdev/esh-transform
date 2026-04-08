package dev.ikm.ike.esh.tinkarizer.intersect.grid;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.impl.AbstractLoader;
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

public class IntersectGridLoader extends AbstractLoader {

	Logger LOG = LoggerFactory.getLogger(IntersectGridLoader.class);

	private static final Concept EC_IDENTIFIER_CONCEPT = Concept.make(PublicIds.of(UUID.fromString("9900bd6e-42d4-4484-80f2-cea339c956b5")));

	public IntersectGridLoader(String composerName, long time, UUID author, UUID module, UUID path) {
		super(composerName, time, author, module, path);
		init();
	}

	private void init() {
		activeSession.compose((ConceptAssembler conceptAssembler) -> conceptAssembler.concept(EC_IDENTIFIER_CONCEPT)
				.attach((FullyQualifiedName fqn) -> fqn.language(TinkarTermV2.ENGLISH_LANGUAGE)
						.text("Event Code Identifier").caseSignificance(TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE)
						.attach((USDialect dialect) -> dialect.acceptability(TinkarTermV2.PREFERRED)))
				.attach((Synonym syn) -> syn.language(TinkarTermV2.ENGLISH_LANGUAGE).text("EC Identifier")
						.caseSignificance(TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE)
						.attach((USDialect dialect) -> dialect.acceptability(TinkarTermV2.PREFERRED)))
				.attach((Definition def) -> def.language(TinkarTermV2.ENGLISH_LANGUAGE)
						.text("Identifier to enable uniqueness between Event Codes.")
						.caseSignificance(TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE)
						.attach((USDialect dialect) -> dialect.acceptability(TinkarTermV2.PREFERRED)))
				.attach((StatedAxiom stated) -> stated.isA(TinkarTermV2.IDENTIFIER_SOURCE)));

		// Attached Root
		Concept eshModel = Concept.make(PublicIds.of(UUID.fromString("f0b69a19-ba4f-4e52-b30e-d998f028f0ab")));
		Concept esRoot = Concept.make(PublicIds.of(UUID.fromString("47e533f4-a3d8-5d5b-826d-24eb09d1f3ab")));
		activeSession.compose(new StatedAxiom().isA(eshModel), esRoot);
	}

	public void loadViewableData(List<ViewableCanonicalRecord> viewableData) {
		viewableData.forEach(data -> {
			Concept concept = Concept.make(PublicIds.of(data.ids()));

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
			if (!data.identifier().isEmpty()) {
				activeSession.compose(new Identifier().source(EC_IDENTIFIER_CONCEPT).identifier(data.identifier()),
						concept);
				semanticCounter.incrementAndGet();
			}
		});
		LOG.info("Finish transforming {} Viewable Data", viewableData.size());
	}

	public void loadNavigableData(List<NavigableCanonicalRecord> navigableData) {
		navigableData.forEach(data -> {
			Concept reference = Concept.make(PublicIds.of(data.childId()));
			List<Concept> parentConcepts = data.parentIds().stream().map(PublicIds::of).map(Concept::make).toList();
			activeSession.compose(new StatedAxiom().isA(parentConcepts.toArray(new Concept[0])), reference);
		});
		LOG.info("Finish transforming {} Navigable Data", navigableData.size());
	}
}
