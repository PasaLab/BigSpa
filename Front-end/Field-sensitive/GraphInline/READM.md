# inline
The entry of program is in ```edu.uci.inline.client.GraphGenerator.java```.

## Running parameters:
|  args   | description  |
|  ----  | ----  |
| args[0]  | The input file of callGraph|
| args[1]  | The file of intragraph|
| args[2]  | Unused|
| args[3]  | Simply pass "file"|
| args[4]  | Output path|
| args[5]  | The input file of callSite|

The inline procedure will finally produce 3 files: final,index_map_info.txt和vertex_info.txt.

### example
Params for running ```mytest1```:

```/mytest/callgraph.txt mytest/interOutput/interGraph linux File mytest/interOutput/final mytest/interOutput/callSite```