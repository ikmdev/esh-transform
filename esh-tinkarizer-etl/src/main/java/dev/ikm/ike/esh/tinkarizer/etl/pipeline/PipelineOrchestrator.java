package dev.ikm.ike.esh.tinkarizer.etl.pipeline;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class PipelineOrchestrator {

	private final List<Pipeline> pipelines;

	public PipelineOrchestrator() {
		this.pipelines = new ArrayList<>();
	}

	public void addPipeline(Pipeline pipeline) {
		this.pipelines.add(pipeline);
	}

	public void runPipelines(File dbPath, String dbName) {
		runViewablePhase();
		runNavigablePhase();
	}

	private void runViewablePhase() {
		for (Pipeline pipeline : pipelines) {
			pipeline.executeViewableETL();
		}
	}

	private void runNavigablePhase() {
		for (Pipeline pipeline : pipelines) {
			pipeline.executeNavigableETL();
		}
	}

}
