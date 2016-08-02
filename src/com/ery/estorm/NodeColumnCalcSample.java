package com.ery.estorm;

import java.util.Map;

import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorString;
import com.ery.estorm.util.AviatorFunction;

public class NodeColumnCalcSample extends NodeColumnCalc {
	static {
		NodeColumnCalcSample test = new NodeColumnCalcSample();
		AviatorFunction.addFunction(test);
		NodeColumnCalc.addNodeColumnCalc(test);
	}

	@Override
	public Object calc(String... f2) {
		return null;
	}

	@Override
	public AviatorObject call(Map env, AviatorObject arg1, AviatorObject arg2) {
		String left = FunctionUtils.getStringValue(arg1, env);
		String left2 = FunctionUtils.getStringValue(arg2, env);
		StringBuffer mstr = new StringBuffer(left);
		mstr = mstr.reverse();
		StringBuffer mstr2 = new StringBuffer(left2);
		mstr.append(mstr2.reverse());
		return new AviatorString(mstr.toString());
	}

	@Override
	public String getName() {
		return "SampleCalc";
	}

}
