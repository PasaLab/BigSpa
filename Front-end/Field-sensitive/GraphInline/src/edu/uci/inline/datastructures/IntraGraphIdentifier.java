package edu.uci.inline.datastructures;

public class IntraGraphIdentifier {
	private String callName;
	private String fileName;
	// private String returnName;

	// private int startLineNum;
	// private int endLineNum;

	public IntraGraphIdentifier() {
	}

	public IntraGraphIdentifier(String callName, String fileName) {
		this.callName = callName;
		this.fileName = fileName;
		// this.returnName = "";
	}

	// public IntraGraphIdentifier(String callName, String fileName, String
	// returnName) {
	// this.callName = callName;
	// this.fileName = fileName;
	// this.returnName = returnName;
	// }

	public IntraGraphIdentifier(String callName, String fileName, int sLn, int eLn) {
		this.callName = callName;
		this.fileName = fileName;
		// this.startLineNum = sLn;
		// this.endLineNum = eLn;
	}

	public String getCallName() {
		return callName;
	}

	public void setCallName(String callName) {
		this.callName = callName;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	// public String getReturnName() {
	// return returnName;
	// }
	//
	// public void setReturnName(String returnName) {
	// this.returnName = returnName;
	// }

	// public int getStartLineNum() {
	// return startLineNum;
	// }
	//
	// public void setStartLineNum(int startLineNum) {
	// this.startLineNum = startLineNum;
	// }
	//
	// public int getEndLineNum() {
	// return endLineNum;
	// }
	//
	// public void setEndLineNum(int endLineNum) {
	// this.endLineNum = endLineNum;
	// }
	//
	// public void setSourceRange(String range_string){
	// String[] nums = range_string.split(",");
	// assert(nums.length == 2);
	// this.startLineNum = Integer.parseInt(nums[0]);
	// this.endLineNum = Integer.parseInt(nums[1]);
	// }

	public int hashCode() {
		int hash = 17;
		hash = 31 * hash + callName.hashCode();
		hash = 31 * hash + fileName.hashCode();
		// hash = 31 * hash + returnName.hashCode();
		// hash = 31*hash + this.startLineNum;
		// hash = 31*hash + this.endLineNum;

		return hash;
	}

	public boolean equals(Object obj) {
		if (!(obj instanceof IntraGraphIdentifier))
			return false;
		if (obj == this)
			return true;
		IntraGraphIdentifier node = (IntraGraphIdentifier) obj;
		return node.callName.equals(this.callName) && node.fileName.equals(this.fileName)
		// && node.returnName.equals(this.returnName)
		// && node.startLineNum == this.startLineNum && node.endLineNum ==
		// this.endLineNum
		;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();

		result.append(callName + "\t");
		result.append(fileName + "\t");
		//result.append(returnName + "\t");
		// result.append(this.startLineNum + "," +
		// this.endLineNum).append("\t");

		return result.toString();
	}
}
