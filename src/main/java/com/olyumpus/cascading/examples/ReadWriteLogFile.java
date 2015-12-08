package com.olyumpus.cascading.examples;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class ReadWriteLogFile {

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Usage : ReadWriteLogFile <<input-file>> <<output-file");
			System.exit(9);
		}

		String inputPath = args[0];
		String outputPath = args[1];

		Fields inFields = new Fields("ip", "time", "request", "response", "size");
		Fields outFields = new Fields("ip", "time", "request", "response", "size");

		Tap inTap = new FileTap(new TextDelimited(inFields, "\t"), inputPath);
		Tap outTap = new FileTap(new TextDelimited(outFields, "\t"), outputPath);

		Pipe copyPipe = new Pipe("copy-pipe");
		FlowDef flowDef = FlowDef.flowDef().addSource(copyPipe, inTap).addTailSink(copyPipe, outTap);

		Flow flow = new LocalFlowConnector().connect(flowDef);
		flow.complete();

	}

}
