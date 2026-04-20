package dev.ikm.ike.esh.tinkarizer.starter.data;

import java.util.UUID;

import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.terms.EntityProxy.Concept;
import dev.ikm.tinkar.terms.TinkarTermV2;

public class ESHStarterData {

	public static final UUID EVENT_CODE_NAMESPACE = UUID.fromString("7291397d-9ead-4f1e-bcd2-3f8facdcf468");
	public static final UUID EVENT_SET_NAMESPACE = UUID.fromString("94282870-9279-46c0-b4ae-82cadddf7b7d");

	public static final Concept ESH_AUTHOR_CONCEPT = Concept
			.make(PublicIds.of(UUID.fromString("5ffd229a-aaa1-4a93-9577-8950f86a2454")));
	public static final UUID ESH_AUTHOR_UUID = ESH_AUTHOR_CONCEPT.uuids()[0];

	public static final Concept ESH_MODULE_CONCEPT = Concept
			.make(PublicIds.of(UUID.fromString("fd7b189b-eeba-4a41-98bb-b101cf80ef02")));
	public static final UUID ESH_MODULE_UUID = ESH_MODULE_CONCEPT.uuids()[0];

	public static final Concept ESH_MODEL_CONCEPT = Concept
			.make(PublicIds.of(UUID.fromString("f0b69a19-ba4f-4e52-b30e-d998f028f0ab")));
	public static final UUID EVENT_CODE_IDENTIFIER_UUID = UUID.fromString("9900bd6e-42d4-4484-80f2-cea339c956b5");
	public static final Concept EVENT_SET_ROOT_CONCEPT = Concept
			.make(PublicIds.of(UUID.fromString("47e533f4-a3d8-5d5b-826d-24eb09d1f3ab")));
	
	public static final UUID DEVELOPMENT_PATH_UUID = TinkarTermV2.DEVELOPMENT_PATH.asUuidArray()[0];

}
