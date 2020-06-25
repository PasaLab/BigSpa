package edu.zuo.setree.intergraph;

//import edu.zuo.callgraph.callGraph;
import soot.jimple.toolkits.callgraph.CallGraph;

import java.io.*;
import java.util.*;

/**
 * Created by wangyifei on 2018/5/1.
 */
class pair extends Object {
	private long left;
	private long right;

	public pair(long left, long right) {
		this.left = left;
		this.right = right;
	}

	public void setLeft(long left) {
		this.left = left;
	}

	public void setRight(long right) {
		this.right = right;
	}

	public void set(long left, long right) {
		this.left = left;
		this.right = right;
	}

	public String toString() {
		return "(" + left + "," + right + ")";
	}

	@Override
	public int hashCode() {
		return this.toString().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof pair) {
			pair o = (pair) obj;
			if (left == o.left && right == o.right) {
				return true;
			}
		}
		return false;
	}
}

class ValueComparator implements Comparator<String> {
	HashMap<String, Long> map = new HashMap<String, Long>();

	public ValueComparator(HashMap<String, Long> map) {
		this.map.putAll(map);
	}

	@Override
	public int compare(String s1, String s2) {
		if (map.get(s1) >= map.get(s2)) {
			return 1;
		} else {
			return -1;
		}
	}
}

public class interGraph {
	public static final File consEdgeGraphFile = new File(
			"intraOutput/consEdgeGraph");
	public static final File var2indexMapFile = new File(
			"intraOutput/var2indexMap");
	public static final File conditionalSmt2File = new File(
			"intraOutput/conditionalSmt2");
	public static final File callGraphIntraFile = new File(
			"intraOutput/callGraph");
	public static long funcIndex = -1;

	private static Map<pair, Long> pair2indexMap = new LinkedHashMap<>();
	private static Map<Long, String> index2varMap = new LinkedHashMap<>();
	private static Map<String, Long> func2indexMap = new LinkedHashMap<>();
	private static Map<Long, Map<String, Long>> funcParamReturn = new LinkedHashMap<>();

	private static HashMap<String, Long> callOrder = new HashMap<>();
	public static Map<String, Set<String>> callMap = new LinkedHashMap<>();

	/**
	 * each line of pair2varMapFile is (funcIndex,inFuncVarIndex) :
	 * outFuncVarIndex
	 */
	public static final File pair2indexMapFile = new File(
			"interOutput/pair2indexMap.txt");

	/**
	 * each line of index2varMapFile is outFuncVarIndex :
	 * funcIndex.inFuncVarIndex.nodeIndex.varName
	 */
	public static final File index2varMapFile = new File(
			"interOutput/index2varMap.txt");

	/**
	 * each line of func2indexMapFile is funcIndex : funcName
	 */
	public static final File func2indexMapFile = new File(
			"interOutput/func2indexMap.txt");

	/**
	 * each line of interGraphFile is outFuncVarIndex outFuncVarIndex label
	 * constraint label: a|e|l|n|o|p|r|s constraint: [T]|[pair,pair] pair:
	 * (funcIndex, nodeIndex)
	 * 
	 * [Assign] a [Load] l [New] n [Callee] c [Param] p [Return] r [Store] s
	 * other e
	 */
	public static final File interGraphFile = new File(
			"interOutput/interGraph.txt");

	/**
	 * each line of interSmt2File is (funcIndex, nodeIndex):constraintString
	 */
	public static final File interSmt2File = new File(
			"interOutput/interSmt2.txt");

	/**
	 * callGraph
	 * 
	 */
	public static final File callGraphFile = new File(
			"interOutput/callGraph.txt");

	public static void genMap() {
		try {
			if (!consEdgeGraphFile.exists()) {
				System.out.println("Error: consEdgeGraph file not exists.");
				return;
			}
			if (!var2indexMapFile.exists()) {
				System.out.println("Error: var2indexMap file not exists.");
				return;
			}
			Scanner consEdgeGraphInput = new Scanner(consEdgeGraphFile);
			Scanner var2indexMapInput = new Scanner(var2indexMapFile);
			System.out.println("-----var2indexMap-----");
			long varIndex = 0;
			while (var2indexMapInput.hasNextLine()) {
				String line = var2indexMapInput.nextLine();
				if (line.length() == 0) {

				} else if (line.startsWith("<")) {
					++funcIndex;
					func2indexMap.put(line, funcIndex);
				} else {
					System.out.println("-#" + varIndex + ":"
							+ Runtime.getRuntime().totalMemory() / 1048576
							+ "m#-");
					String[] tokens = line.split(" : ");
					long right = Long.parseLong(tokens[0]);
					pair2indexMap.put(new pair(funcIndex, right), varIndex);
					index2varMap.put(varIndex, funcIndex + "." + tokens[0]
							+ "." + tokens[1]);
					varIndex++;
				}

			}
			System.out.println("-----consEdgeGraph-----");
			funcIndex = -1;
			while (consEdgeGraphInput.hasNextLine()) {
				String line = consEdgeGraphInput.nextLine();
				if (line.length() == 0) {

				} else if (line.startsWith("<")) {
					++funcIndex;
					System.out.println("-#" + funcIndex + ":"
							+ Runtime.getRuntime().totalMemory() / 1048576
							+ "m#-");
					funcParamReturn.put(funcIndex,
							new LinkedHashMap<String, Long>());
				} else {
					String[] tokens = line.split(", ");
					if (tokens.length == 2) {
						long i = Long.parseLong(tokens[0]);
						funcParamReturn.get(funcIndex).put(tokens[1], i);
					}
				}
			}
			consEdgeGraphInput.close();
			var2indexMapInput.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void printMap() {
		PrintWriter pair2indexMapOut = null;
		PrintWriter index2varMapOut = null;
		PrintWriter func2indexMapOut = null;
		try {
			if (!pair2indexMapFile.exists()) {
				pair2indexMapFile.createNewFile();
			}
			if (!index2varMapFile.exists()) {
				index2varMapFile.createNewFile();
			}
			if (!func2indexMapFile.exists()) {
				func2indexMapFile.createNewFile();
			}

			pair2indexMapOut = new PrintWriter(new BufferedWriter(
					new FileWriter(pair2indexMapFile, true)));
			index2varMapOut = new PrintWriter(new BufferedWriter(
					new FileWriter(index2varMapFile, true)));
			func2indexMapOut = new PrintWriter(new BufferedWriter(
					new FileWriter(func2indexMapFile, true)));

			for (pair p : pair2indexMap.keySet()) {
				pair2indexMapOut.println(p.toString() + " : "
						+ pair2indexMap.get(p));
			}
			for (Long i : index2varMap.keySet()) {
				index2varMapOut.println(i + " : " + index2varMap.get(i));
			}
			for (String f : func2indexMap.keySet()) {
				func2indexMapOut.println(func2indexMap.get(f).toString()
						+ " : " + f);
			}
			pair2indexMapOut.close();
			index2varMapOut.close();
			func2indexMapOut.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void printGraph() {

		try {
			Scanner consEdgeGraphInput = new Scanner(consEdgeGraphFile);
			funcIndex = -1;
			PrintWriter interGraphOut = null;
			PrintWriter interSmt2Out = null;
			if (!interGraphFile.exists()) {
				interGraphFile.createNewFile();
			}
			interGraphOut = new PrintWriter(new BufferedWriter(new FileWriter(
					interGraphFile, true)));
			interSmt2Out = new PrintWriter(new BufferedWriter(new FileWriter(
					interSmt2File, true)));
			while (consEdgeGraphInput.hasNextLine()) {
				String line = consEdgeGraphInput.nextLine();
				System.out.println("#" + line + "#");
				if (line.length() == 0) {

				} else if (line.startsWith("<")) {
					 interGraphOut.println("#################################################");
					++funcIndex;
				} else {
					String[] tokens = line.split(", ");
					if (tokens.length == 2) {
						System.out.println(2);
					} else if (tokens.length == 3) {
						/*
						 * a, b, [Assign] a a, b, [Load] l a, b, [New] n a, b,
						 * [New & Store] b a, b, [Store] s
						 */
						System.out.println(3);
						long first_right = Long.parseLong(tokens[0]);
						long second_right = Long.parseLong(tokens[1]);
						pair p1 = new pair(funcIndex, first_right);
						pair p2 = new pair(funcIndex, second_right);
						long i1 = pair2indexMap.get(p1);
						long i2 = pair2indexMap.get(p2);
						String label = tokens[2];
						switch (tokens[2]) {
						case "[Assign]":
							label = "a";
							break;
						case "[Load]":
							label = "l";
							break;
						case "[New]":
							label = "n";
							break;
						case "[New & Store]":
							label = "b";
							break;
						case "[Store]":
							label = "s";
							break;
						default:
							assert (false);
						}
						interGraphOut.println(i1 + "\t" + i2 + "\t" + label
								+ "\t[T]");
						interGraphOut.println(i2 + "\t" + i1 + "\t-" + label
								+ "\t[T]");
					} else if (tokens.length == 4) {
						/*
						 * a, b, [x, y]
						 */
						System.out.println(4);
						long first_right = Long.parseLong(tokens[0]);
						long second_right = Long.parseLong(tokens[1]);
						pair p1 = new pair(funcIndex, first_right);
						pair p2 = new pair(funcIndex, second_right);
						long i1 = pair2indexMap.get(p1);
						long i2 = pair2indexMap.get(p2);
						long n1_rignt = Long.parseLong(tokens[2].substring(1));
						long n2_right = Long.parseLong(tokens[3].substring(0,
								tokens[3].length() - 1));
						pair p3 = new pair(funcIndex, n1_rignt);
						pair p4 = new pair(funcIndex, n2_right);
						interGraphOut.println(i1 + "\t" + i2 + "\te\t["
								+ p3.toString() + "," + p4.toString() + "]");
						interGraphOut.println(i2 + "\t" + i1 + "\t-e\t["
								+ p3.toString() + "," + p4.toString() + "]");
					} else {
						 interGraphOut.println("+++++++++++++++++++++++++++++++++++++");
						assert (tokens[3] == "[Call]");
						// Do not handle system call
						if (!func2indexMap.containsKey(tokens[2]))
							continue;
						long funcNodeIndex = Long.parseLong(tokens[1]);
						long callFuncIndex = func2indexMap.get(tokens[2]);
						if (callFuncIndex < 0)
							continue;
						String calleeParaSmt2 = "";
						// System.out.println(funcNodeIndex+" "+callFuncIndex);
						// callee o
						if (tokens[4].length() > 2) {
							long first_right = Long.parseLong(tokens[4]
									.substring(1, tokens[4].length() - 1));
							if (!(funcParamReturn.containsKey(callFuncIndex) && funcParamReturn
									.get(callFuncIndex).containsKey("[Callee]")))
								continue;
							long second_right = funcParamReturn.get(
									callFuncIndex).get("[Callee]");
							pair p1 = new pair(funcIndex, first_right);
							pair p2 = new pair(callFuncIndex, second_right);
							long i1 = pair2indexMap.get(p1);
							long i2 = pair2indexMap.get(p2);
							pair p3 = new pair(funcIndex, funcNodeIndex);
							pair p4 = new pair(callFuncIndex, 0);
							interGraphOut
									.println(i1 + "\t" + i2 + "\to\t["
											+ p3.toString() + ","
											+ p4.toString() + "]");
							interGraphOut
									.println(i2 + "\t" + i1 + "\t-o\t["
											+ p3.toString() + ","
											+ p4.toString() + "]");
							calleeParaSmt2 = calleeParaSmt2 + "["
									+ p3.toString() + "," + p4.toString()
									+ "]:(= $D$" + funcIndex + "$Callee"
									+ first_right + " $D$" + callFuncIndex
									+ "$Callee" + second_right + ")";
						}
						// params p
						int paraN = tokens.length - 5;
						if (paraN == 1) {
							if (tokens[5].length() == 2)
								continue;
							long first_right = Long.parseLong(tokens[5]
									.substring(1, tokens[5].length() - 1));
							if (!(funcParamReturn.containsKey(callFuncIndex) && funcParamReturn
									.get(callFuncIndex).containsKey("[Para0]")))
								continue;
							long second_right = funcParamReturn.get(
									callFuncIndex).get("[Para0]");
							pair p1 = new pair(funcIndex, first_right);
							pair p2 = new pair(callFuncIndex, second_right);
							long i1 = pair2indexMap.get(p1);
							long i2 = pair2indexMap.get(p2);
							pair p3 = new pair(funcIndex, funcNodeIndex);
							pair p4 = new pair(callFuncIndex, 0);
							interGraphOut
									.println(i1 + "\t" + i2 + "\tp\t["
											+ p3.toString() + ","
											+ p4.toString() + "]");
							interGraphOut
									.println(i2 + "\t" + i1 + "\t-p\t["
											+ p3.toString() + ","
											+ p4.toString() + "]");
							if (calleeParaSmt2.length() == 0) {
								calleeParaSmt2 = calleeParaSmt2 + "["
										+ p3.toString() + "," + p4.toString()
										+ "]:(= $D$" + funcIndex + "$Para"
										+ first_right + " $D$" + callFuncIndex
										+ "$Para" + second_right + ")";
							} else {
								calleeParaSmt2 = calleeParaSmt2 + "#(= $D$"
										+ funcIndex + "$Para" + first_right
										+ " $D$" + callFuncIndex + "$Para"
										+ second_right + ")";
							}
						} else {
							long first_right = Long.parseLong(tokens[5]
									.substring(1, tokens[5].length()));
							if (!(funcParamReturn.containsKey(callFuncIndex) && funcParamReturn
									.get(callFuncIndex).containsKey("[Para0]")))
								continue;
							long second_right = funcParamReturn.get(
									callFuncIndex).get("[Para0]");
							pair p1 = new pair(funcIndex, first_right);
							pair p2 = new pair(callFuncIndex, second_right);
							long i1 = pair2indexMap.get(p1);
							long i2 = pair2indexMap.get(p2);
							pair p3 = new pair(funcIndex, funcNodeIndex);
							pair p4 = new pair(callFuncIndex, 0);
							interGraphOut
									.println(i1 + "\t" + i2 + "\tp\t["
											+ p3.toString() + ","
											+ p4.toString() + "]");
							interGraphOut
									.println(i2 + "\t" + i1 + "\t-p\t["
											+ p3.toString() + ","
											+ p4.toString() + "]");
							calleeParaSmt2 = calleeParaSmt2 + "#(= $D$"
									+ funcIndex + "$Para" + first_right
									+ " $D$" + callFuncIndex + "$Para"
									+ second_right + ")";
							for (int i = 1; i < paraN - 1; i++) {
								first_right = Long.parseLong(tokens[5 + i]
										.substring(0, tokens[5 + i].length()));
								second_right = funcParamReturn.get(
										callFuncIndex).get("[Para" + i + "]");
								p1 = new pair(funcIndex, first_right);
								p2 = new pair(callFuncIndex, second_right);
								i1 = pair2indexMap.get(p1);
								i2 = pair2indexMap.get(p2);
								p3 = new pair(funcIndex, funcNodeIndex);
								p4 = new pair(callFuncIndex, 0);
								interGraphOut.println(i1 + "\t" + i2 + "\tp\t["
										+ p3.toString() + "," + p4.toString()
										+ "]");
								interGraphOut.println(i2 + "\t" + i1
										+ "\t-p\t[" + p3.toString() + ","
										+ p4.toString() + "]");
								calleeParaSmt2 = calleeParaSmt2 + "#(= $D$"
										+ funcIndex + "$Para" + first_right
										+ " $D$" + callFuncIndex + "$Para"
										+ second_right + ")";
							}

							int paraIndex = paraN - 1;
							first_right = Long
									.parseLong(tokens[5 + paraIndex].substring(
											0,
											tokens[5 + paraIndex].length() - 1));
							second_right = funcParamReturn.get(callFuncIndex)
									.get("[Para" + paraIndex + "]");
							p1 = new pair(funcIndex, first_right);
							p2 = new pair(callFuncIndex, second_right);
							i1 = pair2indexMap.get(p1);
							i2 = pair2indexMap.get(p2);
							p3 = new pair(funcIndex, funcNodeIndex);
							p4 = new pair(callFuncIndex, 0);
							interGraphOut
									.println(i1 + "\t" + i2 + "\tp\t["
											+ p3.toString() + ","
											+ p4.toString() + "]");
							interGraphOut
									.println(i2 + "\t" + i1 + "\t-p\t["
											+ p3.toString() + ","
											+ p4.toString() + "]");
							calleeParaSmt2 = calleeParaSmt2 + "#(= $D$"
									+ funcIndex + "$Para" + first_right
									+ " $D$" + callFuncIndex + "$Para"
									+ second_right + ")";
						}
						if (calleeParaSmt2.length() != 0) {
							interSmt2Out.println(calleeParaSmt2);
						}
						// return r
						if (tokens[0].length() > 2) {
							long second_right = Long.parseLong(tokens[0]
									.substring(1, tokens[0].length() - 1));
							pair p2 = new pair(funcIndex, second_right);
							long i2 = pair2indexMap.get(p2);
							pair p4 = new pair(funcIndex, funcNodeIndex);
							Map<String, Long> t = funcParamReturn
									.get(callFuncIndex);
							for (String s : t.keySet()) {
								if (s.startsWith("[Return")) {
									long first_right = t.get(s);
									long callFuncRetNode = Long.parseLong(s
											.substring(7, s.length() - 1));
									pair p1 = new pair(callFuncIndex,
											first_right);
									long i1 = pair2indexMap.get(p1);
									pair p3 = new pair(callFuncIndex,
											callFuncRetNode);
									interGraphOut.println(i1 + "\t" + i2
											+ "\tr\t[" + p3.toString() + ","
											+ p4.toString() + "]");
									interGraphOut.println(i2 + "\t" + i1
											+ "\t-r\t[" + p3.toString() + ","
											+ p4.toString() + "]");
									interSmt2Out.println("[" + p3.toString()
											+ "," + p4.toString() + "]:(= $D$"
											+ funcIndex + "$Return"
											+ first_right + " $D$"
											+ callFuncIndex + "$Return"
											+ second_right + ")");
								}
							}

						}
						 interGraphOut.println("-----------------------------------------------");
					}
				}
			}

			consEdgeGraphInput.close();
			interGraphOut.close();
			interSmt2Out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static String add_ite(String line) {
		int split1 = line.length();
		int i1 = line.indexOf("(bvand ");
		if (i1 != -1)
			split1 = i1 + 6;
		int i2 = line.indexOf("(bvor ");
		if (i2 != -1 && i2 < split1)
			split1 = i2 + 5;
		int i3 = line.indexOf("(bvxor ");
		if (i3 != -1 && i3 < split1)
			split1 = i3 + 6;
		if (split1 == line.length()) {
			if (line.startsWith("(")) {
				return line;
			} else if (line.charAt(0) >= '0' && line.charAt(0) <= '9') {
				return "#x" + line;
			} else {
				return "$bv$" + line;
			}
		}

		char lineChar[] = line.toCharArray();
		long num = 1;
		int split2 = 0;
		int split3 = 0;
		for (int i = split1 + 1; i < lineChar.length; i++) {
			if (lineChar[i] == '(')
				++num;
			if (lineChar[i] == ')')
				--num;
			if (num == 1 && lineChar[i] == ' ') {
				assert (split2 == 0);
				split2 = i;
			}
			if (num == 0) {
				split3 = i;
				break;
			}
		}
		System.out.println(split1 + " " + split2 + " " + split3 + " " + line);
		assert (split2 > split1 && split3 > split2);
		String p1 = line.substring(0, split1);
		String p2 = add_ite(line.substring(split1 + 1, split2));
		String p3 = add_ite(line.substring(split2 + 1, split3));
		String p4 = "";
		if (split3 + 1 < line.length())
			p4 = " " + add_ite(line.substring(split3 + 1).trim());
		return p1 + " " + p2 + " " + p3 + ")" + p4;
	}

	public static void printSmt2() {
		try {
			if (!conditionalSmt2File.exists()) {
				System.out.println("Error: conditionalSmt2 file not exists.");
				return;
			}
			Scanner conditionalSmt2Input = new Scanner(conditionalSmt2File);
			PrintWriter interSmt2Out = null;
			if (!interSmt2File.exists()) {
				interSmt2File.createNewFile();
			}
			interSmt2Out = new PrintWriter(new BufferedWriter(new FileWriter(
					interSmt2File, true)));
			funcIndex = -1;
			while (conditionalSmt2Input.hasNextLine()) {
				String line = conditionalSmt2Input.nextLine();
				if (line.contains("ishr") || line.contains("ishl")
						|| line.contains("iushr") || line.contains("lshr")
						|| line.contains("lshl") || line.contains("lushr"))
					continue;
				if (line.contains("(iand ") || line.contains("ior ")
						|| line.contains("ixor ") || line.contains("(land ")
						|| line.contains("lor ") || line.contains("lxor ")) {
					line = line.replace("(iand ", "(bvand ")
							.replace("(ior ", "(bvor ")
							.replace("(ixor ", "(bvxor ")
							.replace("(land ", "(bvand ")
							.replace("(lor ", "(bvor ")
							.replace("(lxor ", "(bvxor ");
					line = add_ite(line);
				}
				if (line.length() == 0) {

				} else if (line.startsWith("<")) {
					++funcIndex;
				} else {
					String[] tokens = line.split(":");
					if (tokens[1].startsWith("#")) {

					} else {
						String t = tokens[1].replace("$I", "$I$" + funcIndex)
								.replace("$L", "$L$" + funcIndex)
								.replace("$F", "$F$" + funcIndex)
								.replace("$D", "$D$" + funcIndex)
								.replace("$Z", "$Z$" + funcIndex)
								.replace("$R", "$R$" + funcIndex)
								.replace("$B", "$B$" + funcIndex);
						interSmt2Out.println("(" + funcIndex + "," + tokens[0]
								+ "):" + t);
					}
				}
			}
			conditionalSmt2Input.close();
			interSmt2Out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void printCall() {
		try {
			Scanner callGraphInput = new Scanner(callGraphIntraFile);
			String func = "";
			String line = "";
			funcIndex = -1;
			while (callGraphInput.hasNextLine()) {
				line = callGraphInput.nextLine();
				System.out.println("#" + funcIndex + ":"
						+ Runtime.getRuntime().totalMemory() / 1048576 + "m#");
				if (line.startsWith("<")) {
					++funcIndex;
					func = line;
					callOrder.put(line, (long) 1);
					if (!callMap.containsKey(func)) {
						callMap.put(func, new HashSet<String>());
					}
				} else {
					callOrder.put(line.substring(1), (long) 1);
					callMap.get(func).add(line.substring(1));
				}
			}
			callGraphInput.close();

			/*
			 * consEdgeGraphInput = new Scanner(consEdgeGraphFile); funcIndex =
			 * -1; func = ""; while (consEdgeGraphInput.hasNextLine()) { line =
			 * consEdgeGraphInput.nextLine(); System.out.println("#" + funcIndex
			 * +":"+Runtime.getRuntime().totalMemory()/1048576+"m#");
			 * if(line.length()==0){
			 * 
			 * }else if (line.startsWith("<")) { ++funcIndex; func=line;
			 * //funcSet.add(line); //callGraphOut.println(line); } else {
			 * String[] tokens = line.split(", "); if (tokens.length > 4) { long
			 * start = line.indexOf("<"); long end = line.indexOf(">")+1; String
			 * callsite = line.substring(start,end);
			 * if(callMap.containsKey(callsite)) { System.out.println(callsite);
			 * callMap.get(func).add(callsite); }
			 * //callGraphOut.println("#"+line.substring(start,end)); } } }
			 * consEdgeGraphInput.close();
			 */

			long update = -1;
			long preUpdate;
			while (update != 0) {
				preUpdate = update;
				update = 0;
				for (String key : callMap.keySet()) {
					long order = 1;
					for (String val : callMap.get(key)) {
						order += callOrder.get(val);
					}
					if (order != 1 && order != callOrder.get(key)) {
						callOrder.put(key, order);
						// System.out.println(key);
						++update;
					}
				}
				System.out.println("Totally update " + update);
				if (update == preUpdate) {
					System.out.println("Loop call");
					break;
				}
			}

			Comparator<String> comparator = new ValueComparator(callOrder);
			// TreeMap is a map sorted by its keys.
			// The comparator is used to sort the TreeMap by keys.
			TreeMap<String, Long> result = new TreeMap<String, Long>(comparator);
			result.putAll(callOrder);

			PrintWriter callGraphOut = null;
			if (!callGraphFile.exists()) {
				callGraphFile.createNewFile();
			}
			callGraphOut = new PrintWriter(new BufferedWriter(new FileWriter(
					callGraphFile, true)));
			for (String key : result.keySet()) {
				if (!callMap.containsKey(key))
					continue;
				long size = callMap.get(key).size();
				if (size != 0) {
					String keySplit[] = key.split(" ");
					callGraphOut
							.print(keySplit[1]
									+ " "
									+ keySplit[2].substring(0,
											keySplit[2].lastIndexOf(">"))
									+ " "
									+ keySplit[0].substring(1,
											keySplit[0].indexOf(":")) + "\t"
									+ size);
					for (String val : callMap.get(key)) {
						String valSplit[] = val.split(" ");
						callGraphOut.print("\t"
								+ valSplit[1]
								+ " "
								+ valSplit[2].substring(0,
										valSplit[2].lastIndexOf(">"))
								+ " "
								+ valSplit[0].substring(1,
										valSplit[0].indexOf(":")));
					}
					callGraphOut.println();
				}
			}
			callGraphOut.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String args[]) {
		genMap();
		System.out.println("---------------print Map");
		printMap();
		System.out.println("---------------print intra smt2_constraint");
		printSmt2();
		System.out
				.println("---------------print inter Graph and callee,param,return smt2_constraint");
		printGraph();
		System.out.println("---------------print call Graph");
		// printCall();
		System.out.println("End");

	}
}
