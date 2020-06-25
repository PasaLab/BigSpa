import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class intraFormat {

	public static final String INTRA_GRAPH_START = "#################################################";
	public static final String TEMP_GRAPH_START = "------------------------IntraGraph------------------------";
	public static final String CALL_START = "+++++++++++++++++++++++++++++++++++++";
	public static final String CALL_END = "-----------------------------------------------";
	public static final List<String> calllabel = Arrays.asList("o", "-o", "p", "-p", "r", "-r");

	public static int funcIndex = -1;

	public static final File consEdgeGraphFile = new File("interOutput/interGraph.txt");
	public static final File func2indexMapFile = new File("interOutput/func2indexMap.txt");
	public static final File callinfoFile = new File("callinfo-mapreduce.txt");
	public static final File tempGraphFile = new File(
			"tempGraphFile-mapreduce.txt");
	public static final File finalGraphFile = new File(
			"finalGraphFile-mapreduce.txt");
	public static final File forReturnFile = new File(
			"forReturnFile-mapreduce.txt");

	private static Map<String, String> func2indexMap = new LinkedHashMap<>();
	private static Map<String, List<String>> forreturn = new LinkedHashMap<>();
	private static Map<String, String> metreturn = new LinkedHashMap<>();

	public String classname = "";
	public String methodname = "";
	public String formalparameters = "";

	public static void dealTemp() {
		try {
			boolean iscall = false;
			PrintWriter tempGraph = new PrintWriter(new BufferedWriter(new FileWriter(tempGraphFile, true)));
			PrintWriter callinfo = new PrintWriter(new BufferedWriter(new FileWriter(callinfoFile, true)));
			Scanner consEdgeGraphInput = new Scanner(consEdgeGraphFile);
			while (consEdgeGraphInput.hasNextLine()) {
				String line = consEdgeGraphInput.nextLine();
				if (line.equals(INTRA_GRAPH_START)) {
					if (funcIndex != -1)
						tempGraph.println();
					tempGraph.println("------------------------IntraGraph------------------------");
					iscall = false;
				} else if (line.equals(CALL_START)) {
					iscall = true;
					callinfo.println(CALL_START);
				} else if (line.equals(CALL_END)) {
					iscall = false;
					callinfo.println(CALL_END);
				} else {
					String[] tokens = line.split("\t");
					assert(tokens.length == 4);
					String label = tokens[2];
					if (calllabel.contains(label))
						callinfo.println(line);
					else
						tempGraph.println(line);
				}
			}
			tempGraph.close();
			callinfo.close();
			consEdgeGraphInput.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void dealTempWithCall() {
		try {
			boolean iscall = false;
			int callindex = -1;
			String acall = "";
			String areturn = "";
			String receivemet = "";
			PrintWriter tempGraph = new PrintWriter(new BufferedWriter(new FileWriter(tempGraphFile, false)));
			PrintWriter callinfo = new PrintWriter(new BufferedWriter(new FileWriter(callinfoFile, false)));
			// PrintWriter calledge = new PrintWriter(new BufferedWriter(new
			// FileWriter(callEdgeFile, true)));
			Scanner consEdgeGraphInput = new Scanner(consEdgeGraphFile);
			while (consEdgeGraphInput.hasNextLine()) {
				String line = consEdgeGraphInput.nextLine();
				if (line.equals(INTRA_GRAPH_START)) {
					if (funcIndex != -1)
						tempGraph.println();
					if (!receivemet.equals("")) {
						tempGraph.println("Call Sites: " + callindex + ": [" + receivemet + "\t<" + acall + ">\t<"
								+ areturn + ">]");
						receivemet = "";
						acall = "";
						areturn = "";
						// callindex++;
					}
					tempGraph.println("------------------------IntraGraph------------------------");
					iscall = false;
					System.out.println(funcIndex);
					funcIndex++;
				} else if (line.equals(CALL_START)) {
					if (!receivemet.equals("")) {
						tempGraph.println("Call Sites: " + callindex + ": [" + receivemet + "\t<" + acall + ">\t<"
								+ areturn + ">]");
						receivemet = "";
						acall = "";
						areturn = "";
						// callindex++;
					}
					iscall = true;
					// callinfo.println(CALL_START);
				} else if (line.equals(CALL_END)) {
					if (!receivemet.equals("")) {
						tempGraph.println("Call Sites: " + callindex + ": [" + receivemet + "\t<" + acall + ">\t<"
								+ areturn + ">]");
						receivemet = "";
						acall = "";
						areturn = "";
						// callindex++;
					}
					iscall = false;
					// callinfo.println(CALL_END);
				} else {
					String[] tokens = line.split("\t");
					assert(tokens.length == 4);
					String label = tokens[2];
					String constraint = tokens[3];
					if (calllabel.contains(label)) {
						if (iscall) {
							iscall = false;
							callindex++;
						}
						callinfo.println(callindex + "\t" + tokens[0] + "\t" + tokens[1] + "\t" + label + "\t"
								+ constraint.substring(1, constraint.lastIndexOf("]")));
						if (label.equals("o") || label.equals("p")) {
							acall = acall + tokens[0] + ";";
							receivemet = constraint.substring(constraint.lastIndexOf("(") + 1,
									constraint.lastIndexOf(","));
						} else if (label.equals("r")) {
							areturn = areturn + tokens[1] + ";";
							String retMet = constraint.substring(constraint.indexOf("(") + 1, constraint.indexOf(","));
							if (forreturn.containsKey(retMet)) {
								List<String> value = forreturn.get(retMet);
								if (!value.contains(tokens[0]))
									value.add(tokens[0]);
								forreturn.put(retMet, value);
							} else {
								List<String> value = new ArrayList<String>();
								value.add(tokens[0]);
								forreturn.put(retMet, value);
							}
						}
					} else {
						if (!receivemet.equals("")) {
							tempGraph.println("Call Sites: " + callindex + ": [" + receivemet + "\t<" + acall + ">\t<"
									+ areturn + ">]");
							receivemet = "";
							acall = "";
							areturn = "";
							// callindex++;
						}
						iscall = false;
						tempGraph.println(line);
					}
				}
			}
			if (!receivemet.equals("")) {
				tempGraph.println(
						"Call Sites: " + callindex + ": [" + receivemet + "\t<" + acall + ">\t<" + areturn + ">]");
				receivemet = "";
				acall = "";
				areturn = "";
				// callindex++;
			}
			tempGraph.close();
			callinfo.close();
			consEdgeGraphInput.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void printForReturn() {
		try {
			PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(forReturnFile, false)));
			for (Map.Entry<String, List<String>> entry : forreturn.entrySet()) {
				pw.print(entry.getKey() + ": [");
				for (String r : entry.getValue()) {
					pw.print(r + "\t");
				}
				pw.println("]");
			}
			pw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void readFileToMap() {
		try {
			Scanner func2indexMapInput = new Scanner(func2indexMapFile);
			Scanner forReturnInput = new Scanner(forReturnFile);
			while (func2indexMapInput.hasNextLine()) {
				String line = func2indexMapInput.nextLine();
				String[] tokens = line.split(" : ");
				if (!func2indexMap.containsKey(tokens[0])) {
					func2indexMap.put(tokens[0], tokens[1]);
				}
			}
			while (forReturnInput.hasNextLine()) {
				String line = forReturnInput.nextLine();
				String[] tokens = line.split(": ");
				if (!metreturn.containsKey(tokens[0])) {
					metreturn.put(tokens[0], tokens[1]);
				}
			}

			forReturnInput.close();
			func2indexMapInput.close();
		} catch (IOException e) {

		}
	}

	public static void printGraph() {
		boolean funcStart = false;
		boolean firstMet = true;
		String startmet = "";
		String formalPars = "Formal Parameters: [";
		String formalRets = "Formal Returns: ";
		String edges = "Edges: [";
		int funcIndex = -1;
		List<String> callsitelist = new ArrayList<String>();
		try {
			Scanner tempGraphInput = new Scanner(tempGraphFile);
			PrintWriter finalGraph = new PrintWriter(new BufferedWriter(new FileWriter(finalGraphFile, false)));
			while (tempGraphInput.hasNextLine()) {
				String line = tempGraphInput.nextLine();
				if (line.equals("")) {

				} else if (line.equals(TEMP_GRAPH_START)) {
					funcIndex++;
					// print prev graph information
					if (firstMet) {
						firstMet = false;
					} else {
						if(!startmet.equals("") && !startmet.equals(Integer.toString(funcIndex-1))){
							System.out.println("wrong");
						}
						// end of edge
						if (!edges.equals("Edges: [")) {
							finalGraph.println("]");
							edges = "Edges: [";
						}
						finalGraph.println(formalPars + "]");
						formalPars = "Formal Parameters: [";
						if (metreturn.containsKey(Integer.toString(funcIndex))) {
							finalGraph.println(formalRets + metreturn.get(Integer.toString(funcIndex)));
						}
						for (String s : callsitelist) {
							finalGraph.println(s);
						}
						callsitelist.clear();

						// prev func doesn't have passnode edge
//						if (funcIndex == Integer.parseInt(startmet) + 1) {
//							String[] classMethod = getClassMethod(func2indexMap.get(Integer.toString(funcIndex)));
//							finalGraph.println("File: [" + classMethod[0] + "]");
//							finalGraph.println("Function: [" + classMethod[1] + "]");
//							System.out.println(funcIndex);
//						}
						finalGraph.println("");
					}
					//funcIndex++;
					funcStart = true;
					startmet = "";
					finalGraph.println(TEMP_GRAPH_START);
					String[] classMethod = getClassMethod(func2indexMap.get(Integer.toString(funcIndex)));
					finalGraph.println("File: [" + classMethod[0] + "]");
					finalGraph.println("Function: [" + classMethod[1] + "]");
					System.out.println(funcIndex);
				} else if (line.startsWith("Call Sites: ")) {
					String[] tokens = line.split(": \\[");
					String receive = func2indexMap.get(tokens[1].split("\t")[0]);
					String[] classMethod = getClassMethod(receive);
					// finalGraph.println(
					// tokens[0] + ": [" + classMethod[0] + "\t" +
					// classMethod[1] +
					// tokens[1].substring(tokens[1].indexOf("\t")));
					callsitelist.add(tokens[0] + ": [" + classMethod[0] + "\t" + classMethod[1]
							+ tokens[1].substring(tokens[1].indexOf("\t")));
				} else {
					String[] tokens = line.split("\t");
					String label = tokens[2];
					String constraint = tokens[3];
					if (constraint.startsWith("[") && funcStart) {
						if (constraint.startsWith("[(")) {
							startmet = constraint.substring(constraint.indexOf("(") + 1, constraint.indexOf(","));
//							String[] classMethod = getClassMethod(func2indexMap.get(startmet));
//							finalGraph.println("File: [" + classMethod[0] + "]");
//							finalGraph.println("Function: [" + classMethod[1] + "]");
							//System.out.println(startmet);
						}
						funcStart = false;
						finalGraph.print("Edges: [");
						edges = edges + "has";
					}
					if (!label.contains("-") && constraint.contains("0),(") && constraint.endsWith("1)]")) {
						formalPars = formalPars + tokens[1] + "\t";
					}
					finalGraph.print(tokens[0] + ";" + tokens[1] + ";" + label + ";"
							+ constraint.substring(constraint.indexOf("[") + 1, constraint.indexOf("]")) + "\t");
				}
			}
			if (!edges.equals("Edges: [")) {
				finalGraph.println("]");
				edges = "Edges: [";
			}
			finalGraph.println(formalPars + "]");
			formalPars = "Formal Parameters: [";
			if (metreturn.containsKey(Integer.toString(funcIndex))) {
				finalGraph.println(formalRets + metreturn.get(Integer.toString(funcIndex)));
			}
			for (String s : callsitelist) {
				finalGraph.println(s);
			}
			callsitelist.clear();
			tempGraphInput.close();
			finalGraph.close();
		} catch (IOException e) {

		}
	}

	public static String[] getClassMethod(String str) {
		String[] result = new String[2];
		String[] tokens = str.split(": ");
		result[0] = tokens[0].substring(tokens[0].indexOf("<") + 1);
		result[1] = tokens[1].substring(tokens[1].indexOf(" ") + 1, tokens[1].lastIndexOf(">"));
		return result;
	}

	public static void main(String[] args) {
		dealTempWithCall();
		printForReturn();
		forreturn.clear();
		readFileToMap();
		printGraph();
	}
}
