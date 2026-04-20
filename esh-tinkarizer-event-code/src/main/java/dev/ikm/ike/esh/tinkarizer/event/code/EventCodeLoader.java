package dev.ikm.ike.esh.tinkarizer.event.code;

import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.load.Loader;
import dev.ikm.ike.esh.tinkarizer.etl.session.SessionContext;
import dev.ikm.ike.esh.tinkarizer.starter.data.ESHStarterData;
import dev.ikm.tinkar.common.bind.annotations.names.FullyQualifiedName;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.terms.EntityProxy.Concept;
import dev.ikm.tinkar.terms.EntityProxy.Semantic;
import dev.ikm.tinkar.terms.TinkarTermV2;

public class EventCodeLoader implements Loader {

	Logger LOG = LoggerFactory.getLogger(EventCodeLoader.class);

	private static final Concept EC_IDENTIFIER_CONCEPT = Concept
			.make(PublicIds.of(UUID.fromString("9900bd6e-42d4-4484-80f2-cea339c956b5")));

	// Need to have this method here to ensure the EC Identifier Concept is created
	// before any of the Event Code Concepts are created.
	@Override
	public void initialize(SessionContext sessionContext) {
		sessionContext.activeSession().compose((ConceptAssembler conceptAssembler) -> conceptAssembler.concept(EC_IDENTIFIER_CONCEPT)
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
		sessionContext.activeSession().compose(new StatedAxiom().isA(ESHStarterData.ESH_MODEL_CONCEPT),
				ESHStarterData.EVENT_SET_ROOT_CONCEPT);
	}

	@Override
	public void loadViewableData(SessionContext sessionContext, List<ViewableCanonicalRecord> viewableData) {
		viewableData.forEach(data -> {
			Concept concept = Concept.make(PublicIds.of(data.conceptId()));

			// Create Concept Active or Inactive
			if (data.isActive()) {
				sessionContext.activeSession()
						.compose((ConceptAssembler conceptAssembler) -> conceptAssembler.publicId(concept.publicId()));
			} else {
				sessionContext.inactiveSession()
						.compose((ConceptAssembler conceptAssembler) -> conceptAssembler.publicId(concept.publicId()));
			}

			// Create FQN Semantic
			Semantic fqn = Semantic.make(PublicIds.newRandom());
			sessionContext.activeSession().compose(new FullyQualifiedName().semantic(fqn).language(TinkarTermV2.ENGLISH_LANGUAGE)
					.text(data.fqn()).caseSignificance(TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE), concept);
			sessionContext.activeSession().compose(new USDialect().acceptability(TinkarTermV2.PREFERRED), fqn);

			// Create SYN Semantic
			Semantic syn = Semantic.make(PublicIds.newRandom());
			sessionContext.activeSession().compose(new Synonym().semantic(syn).language(TinkarTermV2.ENGLISH_LANGUAGE)
					.text(data.syn().isEmpty() ? data.fqn() : data.syn())
					.caseSignificance(TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE), concept);
			sessionContext.activeSession().compose(new USDialect().acceptability(TinkarTermV2.PREFERRED), syn);

			// Create DEF Semantic
			Semantic def = Semantic.make(PublicIds.newRandom());
			sessionContext.activeSession().compose(new Definition().semantic(def).language(TinkarTermV2.ENGLISH_LANGUAGE)
					.text(data.def().isEmpty() ? data.fqn() : data.def())
					.caseSignificance(TinkarTermV2.DESCRIPTION_NOT_CASE_SENSITIVE), concept);
			sessionContext.activeSession().compose(new USDialect().acceptability(TinkarTermV2.PREFERRED), def);

			// Create Identifier Semantic
			if (!data.identifier().isEmpty()) {
				sessionContext.activeSession().compose(new Identifier().source(EC_IDENTIFIER_CONCEPT).identifier(data.identifier()),
						concept);
			}
		});
	}

	@Override
	public void loadNavigableData(SessionContext sessionContext, List<NavigableCanonicalRecord> navigableData) {
		navigableData.forEach(data -> {
			Concept reference = Concept.make(PublicIds.of(data.childId()));
			List<Concept> parentConcepts = data.parentIds().stream().map(PublicIds::of).map(Concept::make).toList();
			sessionContext.activeSession().compose(new StatedAxiom().isA(parentConcepts.toArray(new Concept[0])), reference);
		});
	}
}
