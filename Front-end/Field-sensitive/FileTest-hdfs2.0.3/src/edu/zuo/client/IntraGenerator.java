package edu.zuo.client;

import edu.zuo.pegraph.PEGGenerator;
import soot.PackManager;
import soot.Transform;
import soot.options.Options;

public class IntraGenerator {
	// generator
	public static int Generator_ID = 0;

	public static void main(String[] args) {
		// parse arguments
		// TODO

		// set options
		Options.v().setPhaseOption("jb", "use-original-names:true");
		Options.v().setPhaseOption("tag", "off");
		Options.v().set_output_format(Options.output_format_jimple);
		Options.v().set_keep_line_number(true);
		Options.v().set_prepend_classpath(true);

		// add phase
		Transform trans = null;
		switch (Generator_ID) {
		case 0:
			PEGGenerator peggenerator = new PEGGenerator();
			trans = new Transform("jtp.peggenerator", peggenerator);
			break;
		case 1:

			break;
		case 2:
			break;
		default:
			System.err.println("wrong generator!!!");
			System.exit(0);
		}

		PackManager.v().getPack("jtp").add(trans);

		// run
		soot.Main.main(args);

	}

}
