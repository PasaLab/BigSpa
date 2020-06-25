package edu.uci.inline.callgraph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.inline.datastructures.IntraGraphIdentifier;

/**
 * @author Kai Wang
 *
 * Created by Mar 1, 2016
 */
public class CallGraph {
	
	private final List<CallGraphNode> callGraphInfoList;
	
	/**
	 * In callgraph file, each line corresponds to one function node, <file_name "\t" function_name>
	 * */
	public CallGraph(File callGraphFile){
		callGraphInfoList = new ArrayList<CallGraphNode>();
		loadCallGraph(callGraphFile);
	}

	private void loadCallGraph(File callGraphFile) {
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(callGraphFile));
			String ln;
			CallGraphNode callGraphNode = null;
			IntraGraphIdentifier identifier = null;
			ArrayList<IntraGraphIdentifier> listOfIdentifier = null;
			while((ln = reader.readLine()) != null) {
//				System.out.println("this is input string:" + ln);
				String[] info = ln.split("\t");
				// get func name and file name
				String[] names = info[0].split(" ");
				// names[0] for func name, names[1] for file name
				identifier = new IntraGraphIdentifier(names[0], names[1]);
				// get size of callees
				int size = Integer.parseInt(info[1]);
				
				listOfIdentifier = new ArrayList<IntraGraphIdentifier>();
				if(size > 0) {
					for(int index = 0; index < size; index++) {
						String[] calleeNames = info[index + 2].split(" ");
						listOfIdentifier.add(new IntraGraphIdentifier(calleeNames[0], calleeNames[1]));
					}
					
				}
				callGraphNode = new CallGraphNode(identifier, listOfIdentifier);
				callGraphInfoList.add(callGraphNode);
//				System.out.println(callGraphNode);
			}
			
			reader.close();
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public List<CallGraphNode> getCallGraphInfoList() {
		return callGraphInfoList;
	}
	
	
}
