This repo implements the Nested B-Tree index structure [1]

## Compile
To compile the project, run make in the build folder. This creates a library that can be used by other c++ programs. 
## Run
A sample program to use NBTrees is avaiable in the test folder. Run make in the test folder. Note that LD_LIBRARAY_PATH needs to be set to the location of the compiled library. 

### Configuration
NBTree configuration needs to be set in a config file. A sample config file, tree-confing.txt, is available in the test folder. In the config file, DATA_PREFIX needs to be set to the location where data is to be stored.

## References
[1] Sepanta Zeighami, and Raymond Chi-Wing Wong. "Bridging the Gap Between Theory and Practice on Insertion-Intensive Database." arXiv preprint arXiv:2003.01064 (2020).
