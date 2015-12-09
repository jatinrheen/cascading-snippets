package com.olyumpus.cascading.examples;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class AuthorsKeywordUpper {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) {
		parseArgs(args);

		String inputPath = args[0];
		String outputPath = args[1];

		Fields inFields = new Fields("author", "organization", "document", "keyword");
		Fields outFields = new Fields("keyword","author");
		
		Scheme inScheme = new TextDelimited(inFields, true,",");
		Scheme outScheme = new TextDelimited(outFields, true, "|");

		// Define the Taps and Pipes
		Tap inTap = new FileTap(inScheme, inputPath);
		Tap outTap = new FileTap(outScheme, outputPath);
		
		FlowDef flowdef = createWorkFlow(inTap, outTap);
		Properties properties = AppProps.appProps().buildProperties();
		Flow flow = new LocalFlowConnector(properties).connect(flowdef);
		flow.complete();

	}

	private static FlowDef createWorkFlow(Tap inTap, Tap outTap) {
		Pipe upperPipe = new Pipe("Change-Fields-Upper");
		upperPipe = new Each(upperPipe,new UpperCaseFunction(), Fields.RESULTS);
		FlowDef flowdef = FlowDef.flowDef().addSource(upperPipe, inTap).addTailSink(upperPipe, outTap);
		return flowdef;
	}

	private static void parseArgs(String[] args) {
		if (args.length != 2) {
			System.err.println("Usage : ReadAuthorsFile <<input-file>> <<output-file>>");
			System.exit(1);
		}
	}

}
