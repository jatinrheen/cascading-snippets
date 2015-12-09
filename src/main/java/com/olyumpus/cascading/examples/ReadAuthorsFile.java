package com.olyumpus.cascading.examples;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class ReadAuthorsFile {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) {
		parseArgs(args);

		String inputPath = args[0];
		String outputPath = args[1];

		Fields inFields = new Fields("author", "organization", "document", "keyword");
		Fields outFields = new Fields("author", "organization", "document", "keyword");

		Scheme inScheme = new TextDelimited(inFields, ",");
		Scheme outScheme = new TextDelimited(outFields, ",");

		// Define the Taps and Pipes
		Tap inTap = new FileTap(inScheme, inputPath);
		Tap outTap = new FileTap(outScheme, outputPath);
		Pipe copyPipe = new Pipe("read-authors-file");

		// Configure Flow and Run
		FlowDef flowdef = FlowDef.flowDef().addSource(copyPipe, inTap).addTailSink(copyPipe, outTap);
		Properties properties = AppProps.appProps().buildProperties();
		Flow flow = new LocalFlowConnector(properties).connect(flowdef);
		flow.complete();

	}

	private static void parseArgs(String[] args) {
		if (args.length != 2) {
			System.err.println("Usage : ReadAuthorsFile <<input-file>> <<output-file>>");
			System.exit(1);
		}
	}

}
