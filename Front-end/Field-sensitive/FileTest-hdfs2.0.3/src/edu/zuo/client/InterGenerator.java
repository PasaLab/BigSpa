package edu.zuo.client;

import java.util.Map;

import edu.zuo.cg.CGGenerator;
import edu.zuo.pegraph.PEGGenerator;
import soot.PackManager;
import soot.Scene;
import soot.SceneTransformer;
import soot.SootClass;
import soot.Transform;
import soot.options.Options;
import soot.util.Chain;

public class InterGenerator {

	public static int Inter_ID = 0;

	public static void main(String[] args) {
		// options
		Options.v().set_whole_program(true);
		Options.v().setPhaseOption("cg", "enabled:true");
		Options.v().setPhaseOption("cg.spark", "on");
		Options.v().setPhaseOption("wjpp", "enabled:false");
		Options.v().setPhaseOption("wjap", "enabled:false");
		Options.v().setPhaseOption("jap", "enabled:false");
		Options.v().setPhaseOption("jb", "use-original-names:true");
		Options.v().setPhaseOption("tag", "off");
		Options.v().set_keep_line_number(true);
		Options.v().set_output_format(Options.output_format_jimple);
		Options.v().set_prepend_classpath(true);

		// add phase
		Transform trans = null;
		switch (Inter_ID) {
		case 0:
			CGGenerator cggenerator = new CGGenerator();
			trans = new Transform("wjtp.cggenerator", cggenerator);
			break;
		case 1:

			break;
		case 2:
			break;
		default:
			System.err.println("wrong generator!!!");
			System.exit(0);
		}

		PackManager.v().getPack("wjtp").add(trans);

		/**
		 * args should be in the following format:
		 * "-cp path_of_classes_analyzed class_names" e.g., -cp
		 * E:\Workspace\ws_program\taintanalysis\bin\ InterTest HelloWorld
		 */
		soot.Main.main(args);
	}

}
