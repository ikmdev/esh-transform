package dev.ikm.ike.esh.tinkarizer.etl.pipeline;

import dev.ikm.ike.esh.tinkarizer.etl.extract.Extractor;
import dev.ikm.ike.esh.tinkarizer.etl.initialize.Initializer;
import dev.ikm.ike.esh.tinkarizer.etl.load.Loader;
import dev.ikm.ike.esh.tinkarizer.etl.transform.Transformer;
import dev.ikm.ike.esh.tinkarizer.etl.validation.Validator;
import dev.ikm.ike.esh.tinkarizer.etl.verification.Verification;

public record PipelineConfig(
		String pipelineName,
		Initializer initializer,
		Extractor extractor,
		Transformer transformer,
		Validator validator,
		Loader loader,
		Verification verification) {
	
}
