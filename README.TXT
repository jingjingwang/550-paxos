Jingjing Wang 1122602 jwang@cs.washington.edu
Haichen Shen 1222457 haichen@cs.washington.edu


*How to launch the service

you could either launch servers one by one, by:
java PaxosServer [serverID (starts from 0)] [numOfServers]
e.g.
java PaxosServer 0 3
will launch server 0 in totally 3 servers

or

you could launch multiply servers together by:
java LockService [numOfServers]
e.g.
java LockService 3
will launch 3 servers

The first approach is more easy to check running status, inputs, outputs, etc.
The second approach is more convenient for launching multiply servers.

Other things, please see the write-up.

*How to change the configuration
    please check the first few lines in the class PaxosServer. These are all the arguments that you can specify to control the whole service, and are well-commented with their functionalities. If you want to change the configuration, please save and recompile it.
