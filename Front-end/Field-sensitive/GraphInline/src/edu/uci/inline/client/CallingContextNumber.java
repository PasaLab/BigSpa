package edu.uci.inline.client;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.uci.inline.callgraph.CallGraph;
import edu.uci.inline.callgraph.CallGraphNode;
import edu.uci.inline.datastructures.IntraGraphIdentifier;

public class CallingContextNumber {

	
	public static int collectNumber(CallGraph callGraph){
		int numberOfCallContexts = 0;
		Map<IntraGraphIdentifier, Integer> map = new HashMap<IntraGraphIdentifier, Integer>();
		Set<IntraGraphIdentifier> set = new HashSet<IntraGraphIdentifier>();
		
		for(CallGraphNode callNode: callGraph.getCallGraphInfoList()){
			IntraGraphIdentifier caller = callNode.getCaller();
			if(!isEmpty(caller)){
				set.add(caller);
				
				int count_caller = 0;
				
				List<IntraGraphIdentifier> callees = callNode.getCallees();
				for(IntraGraphIdentifier callee: callees){
					if(!isEmpty(callee)){
						set.remove(callee);
						count_caller++;
						if(map.containsKey(callee)){
							int count_callee = map.get(callee);
							count_caller += count_callee;
						}
					}
					
				}
				
				map.put(caller, count_caller);
				
			}
		}
		
		
		for(IntraGraphIdentifier intra: set){
			numberOfCallContexts += map.get(intra);
		}
		
		return numberOfCallContexts;
	}
	
	
	
	private static boolean isEmpty(IntraGraphIdentifier callee) {
		// TODO Auto-generated method stub
		return callee.getFileName().equals("FileNameEmpty") || callee.getCallName().equals("ExternalFunction");
	}



	public static void main(String[] args) {
		String mode = args[0];
		if(mode.equals("Files")){
			long total_count = 0;
			for(int i = 1; i < args.length; i++){
				CallGraph callGraphParser = new CallGraph(new File(args[i]));
				int count = collectNumber(callGraphParser);
				System.out.println(new File(args[i]).getName() + "\t\t" + count);
				total_count += count;
			}
			System.out.println("Total:\t\t" + total_count);
		}
		else if(mode.equals("Folder")){
			for(int i = 1; i < args.length; i++){
				long total_count = 0;
				File folder = new File(args[i]);
				System.out.println(folder.getName());
				File[] files = folder.listFiles();
				for(File file: files){
					CallGraph callGraphParser = new CallGraph(file);
					int count = collectNumber(callGraphParser);
					System.out.println(file.getName() + "\t\t" + count);
					total_count += count;
				}
				System.out.println("Total:\t\t" + total_count);
				System.out.println();	
			}
		}
		else{
			throw new RuntimeException("Wrong mode!!!");
		}
		
		
		
	}
}
