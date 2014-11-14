
Kao: a Scheduling Framework for OAR
====================================

Goal
----
  Provide meta-scheduler and scheduler alernatives to OAR

Main features
--------------

- Python as script language
- Same data structures of Kamelot
    - intervals to represent resource sets
    - available resources are timely arranged through contiugous slots.
- Other choices (high level programs' structures) follow those from the Perl version.
- The first version will be without DB interaction (it'll be just a demonstrator) also
1rst scheduler will not support : 
    - dependencies, fairsharing, timesharing, placeholder, quotas, resource always added,  
and will support :
    - hierarchy, container
