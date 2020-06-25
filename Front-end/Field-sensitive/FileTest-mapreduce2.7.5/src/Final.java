import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import soot.coffi.class_element_value;
import soot.coffi.constant_element_value;

public class Final {
	Map<Integer, Integer> v2vMap = new HashMap<Integer, Integer>();
	protected static final int BUFFER_LIMIT = 501 * 1024 * 1024;
	int newIndex = 0;
	
	public static void main(String[] args) {
		StringBuilder builder = new StringBuilder(2 * 1024 * 1024);
		int eNum = 0;
		int oNum = 0;
		int paNum = 0;
		int rNum = 0;
		int nNum = 0;
		int aNum = 0;
		int gNum = 0;
		int pNum = 0;
		List<Edge> edgeList = new ArrayList<>();
		Final f = new Final();

		BufferedReader reader = null;
		try {
			String line;
			reader = new BufferedReader(new FileReader(
					new File("final-mapreduce")));
			while ((line = reader.readLine()) != null) {
				if (line.equals("")) {
					System.err.println("k");
					continue;
				}
				//System.out.println(line);
				String[] split = line.split("\t");
				assert(split.length == 3);
				//System.err.println(line);
				int start = Integer.parseInt(split[0]);
				int end = Integer.parseInt(split[1]);
				String label = split[2];
				if (label.equals("e") || label.equals("-e")) {
					eNum ++;
				} else if (label.equals("o") || label.equals("-o")) {
					label = label.replace("o", "a");
					oNum ++;
				} else if (label.equals("p") || label.equals("-p")) {
					label = label.replace("p", "a");
					paNum ++;
				} else if (label.equals("r") || label.equals("-r")) {
					label = label.replace("r", "a");
					rNum ++;
				} else if (label.equals("n") || label.equals("-n")) {
					nNum ++;
				} else if (label.equals("a") || label.equals("-a")) {
					aNum ++;
				} else if (label.startsWith("[g(") || label.startsWith("-[g(")) {
					label = label.replace("[", "");
					label = label.replace("]", "");
					System.err.println(label);
					gNum ++;
				} else if (label.startsWith("[p(") || label.startsWith("-[p(")) {
					label = label.replace("[", "");
					label = label.replace("]", "");
					System.err.println(label);
					pNum ++;
				} else {
					System.err.println(line);
					System.err.println(label);
				}
				Edge edge = f.createEdge(start, end, label);
				edgeList.add(edge);
			}
			
		} catch(Exception e) {
			e.printStackTrace();
		}
		System.out.println(eNum + " " + oNum + " " + paNum +
				" " + rNum + " " + nNum + " " + aNum + " " + gNum + " " + pNum);
		System.out.println(edgeList.size());
		
		String fileName = "final-mapreduce-new";
		StringBuilder graphBuilder = new StringBuilder(BUFFER_LIMIT);
		for (Edge edge: edgeList){
			if (graphBuilder.length() >= BUFFER_LIMIT - 1024 * 1024) {
				f.exportFile(graphBuilder, fileName);
				graphBuilder = new StringBuilder(BUFFER_LIMIT);
			}
			graphBuilder.append(edge.start);
			graphBuilder.append("\t");
			graphBuilder.append(edge.end);
			graphBuilder.append("\t");
			graphBuilder.append(edge.label);
			graphBuilder.append("\n");
		}
		f.exportFile(graphBuilder, fileName);
		
		
		StringBuilder builder1 = new StringBuilder(2 * 1024 * 1024);
		BufferedReader reader1 = null;
		int lineNum = 0;
		try {
			String line;
			reader1 = new BufferedReader(new FileReader(
					new File("final-mapreduce-new")));
			while ((line = reader1.readLine()) != null) {
				//System.out.println(line);
				assert(line.split("\t").length==3);
				lineNum ++;
			}
		}
		catch (Exception e) {
		}
		System.out.println(lineNum + " " +edgeList.size() );
	}
	
	public Edge createEdge(int start, int end, String label) {
		Edge e = new Edge();
		e.start = start;
		e.end= end;
		e.label = label;
		return e;
	}
	
	public int getIndex(int v) {
		if (v2vMap.get(v) == null) {
			v2vMap.put(v, newIndex);
			newIndex++;
		}
		return v2vMap.get(v);
	}
	
	public class Edge {
		public int start;
		public int end;
		public String label;
	}
	
	public void exportFile(StringBuilder info, String fileName) {
		PrintWriter out = null;
		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter(new File(fileName), true)));
			out.println(info.toString());
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

}
