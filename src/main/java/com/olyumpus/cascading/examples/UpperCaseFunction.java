package com.olyumpus.cascading.examples;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "serial", "rawtypes" })
public class UpperCaseFunction extends BaseOperation implements Function {

	public UpperCaseFunction() {
		super(new Fields("author", "organization", "document","keyword"));
	}

	@Override
	public void operate(FlowProcess argFlowProcess, FunctionCall argFunctionCall) {
		Tuple result = new Tuple();
		String temp="";
		for (Comparable comparable : fieldDeclaration) {
			temp=argFunctionCall.getArguments().getString(comparable).toUpperCase();
			result.add(temp);
		}
		argFunctionCall.getOutputCollector().add(result);
	}
}
