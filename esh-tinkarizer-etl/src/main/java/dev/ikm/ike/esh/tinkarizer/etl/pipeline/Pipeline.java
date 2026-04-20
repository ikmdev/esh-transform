package dev.ikm.ike.esh.tinkarizer.etl.pipeline;

public interface Pipeline {

	String getName();

	void performInitialization();

	void performExtraction();

	void performTransformation();

	void performValidation();

	void performLoad();

	void performVerification();

	void run();
}