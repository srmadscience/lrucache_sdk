# README #

This README would normally document whatever steps are necessary to get your application up and running.

### What is this repository for? ###

* Quick summary
* Version
* [Learn Markdown](https://bitbucket.org/tutorials/markdowndemo)

### How do I get set up? ###

* Summary of set up
* Configuration
* Dependencies
* Database configuration
* How to run tests
* Deployment instructions


### Task list for adding a new DB for LRU / UpsertExport ###

Assuming your target DB is called 'genericDB':

* Create a new github project called 'genericDBUtil' 
* Create a Java class called 'org.voltdb.seutils.wranglers.genericDB.GenericDBWrangler' that implements org.voltdb.seutils.wranglers.DatabaseWranglerIFace .




### Writing LRU server side code ###

Rule 1: It's not as easy as you think.
Rule 2: See Rule 1.

#### LAST_USE_DATE and LAST_FLUSH_DATE ####

#### You can't delete and flush at the same time ####


#### Things to test ####

Assumption: You are testing our LRU cache in conjunction with our upsert subsystem.

As a general rule VoltDB is much faster than everything else.  

The 'happy path' is relatively easy. Special scenarios that aren't obvious are:

##### Downstream DB server goes offline #####

* VoltDB must cope gracefully, measure errors and not flood the volt.log with crap.
* Upsert process must re-connect when DB comes back on line.
* Upsert process must not lose data while DB is off line.
* Failure of downstream DB should not propagate back up to VoltDB

##### Downstream DB server is overloaded #####

* VoltDB must cope gracefully, measure errors and not flood the volt.log with crap.
* VoltDB must recover gracefully when downstream DB is better able to cope.
* Upsert process must not lose data while DB is off line.
* Failure of downstream DB should not propagate back up to VoltDB
* LRU client must either reject requests when overloaded.

##### VoltDB is overloaded #####

* VoltDB must cope gracefully, measure errors and not flood the volt.log with crap.
* VoltDB must recover gracefully when it is better able to cope.
* Upsert process must not lose data.
* LRU client must either reject requests when overloaded.

### Who do I talk to? ###

* Repo owner or admin
* Other community or team contact