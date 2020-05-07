# Lab 0: Environment Preparation
***Due: Fri, Mar 27th, 12:55 PM***

In the lab assignments you will write a basic database management system called SimpleDB. For this lab, you will focus on preparing necessary tools and environments for the assignments; in future labs, you will add support for the core modules required to access stored data on disk, as well as basic operators, transactions, locking, and concurrent queries.

[SimpleDB](http://www.cs.utah.edu/~lifeifei/acmdb19/simpledb/doc.html) is written in Java. We will provide you with a set of mostly unimplemented classes and interfaces. You will need to write the code for these classes. We will grade your code by running a set of system tests written using [JUnit](https://junit.org/junit5/). These tests will be provided to you so do not worry too much about grading.

The whole working pipeline will be:

Write code -> Test locally -> Upload code to github -> Use azure pipeline to get estimated results on grading system.

The remainder of this document gives you instructions about how to start coding, how to hand in your lab and how to test it on the same runtime environment of grading system. We <b>strongly recommend</b> that you start as early as possible on this lab.

## 1. Software preparation
Make sure that you have correctly installed the following softwares:

* JDK 1.8.* (openjdk-8)
* Ant
* Git

We will provide example installation guide for Ubuntu and Windows. You can try any way you like as long as you make these softwares installed.

### Ubuntu 
```console
$ sudo apt update 
$ sudo apt install openjdk-8-jdk
$ sudo apt install ant
$ sudo apt install git
```
<b>Note:</b> This is also the official test environment we will use.

### Windows
* [JDK 1.8.* Installation](https://www.oracle.com/java/technologies/javase-jdk8-downloads.html)
* [Ant Installation](https://ant.apache.org/manual/install.html)
* [Git Installation](https://git-scm.com/download/win)

## 2. Local test
SimpleDB uses the [Ant build tool](http://ant.apache.org/) to compile the code and run tests. Ant is similar to [make](http://www.gnu.org/software/make/manual/), but the build file is written in XML and is somewhat better suited to Java code. There are 2 kind of tests: unit test and system test, while the former is to smooth your development, and the latter is to do end-to-end test. 

We will provide 3 ways to do local test: command line, using Eclipse and using Intellij.

<b>Note:</b> Windows users can also use the command line tool if you follow the guide above.

### 2.1 Command line
Download the code from [acmdb-lab0.tar.gz](assets/acmdb-lab0.tar.gz) and untar it. For example: 
```console
$ tar xvzf acmdb-lab0.tar.gz
$ cd acmdb-lab0
```

#### 2.1.1 Run unit test
To run the unit tests use the `test` build target:
```console
$ cd acmdb-lab0
$ # run all unit tests
$ ant test
$ # run a specific unit test
$ ant runtest -Dtest=TupleTest
```

You should see output similar to:
```console
# build output...

test:
    [junit] Running simpledb.CatalogTest
    [junit] Testsuite: simpledb.CatalogTest
    [junit] Tests run: 2, Failures: 0, Errors: 2, Time elapsed: 0.037 sec
    [junit] Tests run: 2, Failures: 0, Errors: 2, Time elapsed: 0.037 sec

# ... stack traces and error reports ...
```

The output above indicates that two errors occurred during compilation; this is because the code we have given you doesn't yet work. As you complete parts of the lab, you will work towards passing additional unit tests. If you wish to write new unit tests as you code, they should be added to the `test/simpledb` directory.

For more details about how to use Ant, see the [manual](http://ant.apache.org/manual/). The [Running Ant](http://ant.apache.org/manual/running.html) section provides details about using the ant command. However, the quick reference table below should be sufficient for working on the labs.

Command | Description
--- | ---
ant|Build the default target (for simpledb, this is dist).
ant -projecthelp|List all the targets in `build.xml` with descriptions.
ant dist|Compile the code in src and package it in `dist/simpledb.jar`.
ant test|Compile and run all the unit tests.
ant runtest -Dtest=testname|Run the unit test named `testname`.
ant systemtest|Compile and run all the system tests.
ant runsystest -Dtest=testname|Compile and run the system test named `testname`.

#### 2.1.2 Run system test
We have also provided a set of system tests. These tests are structured as JUnit tests that live in the `test/simpledb/systemtest` directory. To run all the system tests, use the `systemtest` build target:
```console
$ ant systemtest

# ... build output ...

    [junit] Testcase: testSmall took 0.017 sec
    [junit] 	Caused an ERROR
    [junit] expected to find the following tuples:
    [junit] 	19128
    [junit] 
    [junit] java.lang.AssertionError: expected to find the following tuples:
    [junit] 	19128
    [junit] 
    [junit] 	at simpledb.systemtest.SystemTestUtil.matchTuples(SystemTestUtil.java:122)
    [junit] 	at simpledb.systemtest.SystemTestUtil.matchTuples(SystemTestUtil.java:83)
    [junit] 	at simpledb.systemtest.SystemTestUtil.matchTuples(SystemTestUtil.java:75)
    [junit] 	at simpledb.systemtest.ScanTest.validateScan(ScanTest.java:30)
    [junit] 	at simpledb.systemtest.ScanTest.testSmall(ScanTest.java:40)

# ... more error messages ...
```

This indicates that this test failed, showing the stack trace where the error was detected. To debug, start by reading the source code where the error occurred. When the tests pass, you will see something like the following:

```console
$ ant systemtest

# ... build output ...

    [junit] Testsuite: simpledb.systemtest.ScanTest
    [junit] Tests run: 3, Failures: 0, Errors: 0, Time elapsed: 7.278 sec
    [junit] Tests run: 3, Failures: 0, Errors: 0, Time elapsed: 7.278 sec
    [junit] 
    [junit] Testcase: testSmall took 0.937 sec
    [junit] Testcase: testLarge took 5.276 sec
    [junit] Testcase: testRandom took 1.049 sec

BUILD SUCCESSFUL
Total time: 52 seconds
```


### 2.2 Eclipse
[Eclipse](https://www.eclipse.org/) is a graphical software development environment that you might be more comfortable with working in.

#### Setting the Lab Up in Eclipse
* Once Eclipse is installed, start it, and note that the first screen asks you to select a location for your workspace (we will refer to this directory as `$W`).
* On the file system, copy `acmdb-lab0.tar.gz` to `$W/acmdb-lab0.tar.gz`. Un-GZip and un-tar it, which will create a directory `$W/acmdb-lab0` (to do this, you can type `tar -pzxvf acmdb-lab0.tar.gz`).
* In Eclipse, select "Import Project" and "Select Import from Directory" and navigate to the directory `acmdb-lab0`. Note that we have already included .project and .classpath file in the tar.gz file so that you can import this as an Eclipse project easily.
* Click finish, and you should be able to see "acmdb-lab0" as a new project in the Project Explorer tab on the left-hand side of your screen. Opening this project reveals the directory structure discussed above - implementation code can be found in "src," and unit tests and system tests found in "test."

#### Running Individual Unit and System Tests

To run a unit test or system test (both are JUnit tests, and can be initialized the same way), go to the Package Explorer tab on the left side of your screen. Under the "acmdb-lab0" project, open the "test" directory. Unit tests are found in the "simpledb" package, and system tests are found in the "simpledb.systemtests" package. To run one of these tests, select the test (they are all called *Test.java - don't select TestUtil.java or SystemTestUtil.java), right click on it, select "Run As," and select "JUnit Test." This will bring up a JUnit tab, which will tell you the status of the individual tests within the JUnit test suite, and will show you exceptions and other errors that will help you debug problems.

#### Running Ant Build Targets

If you want to run commands such as "ant test" or "ant systemtest," right click on build.xml in the Package Explorer. Select "Run As," and then "Ant Build..." (note: select the option with the ellipsis (...), otherwise you won't be presented with a set of build targets to run). Then, in the "Targets" tab of the next screen, check off the targets you want to run (probably "dist" and one of "test" or "systemtest"). This should run the build targets and show you the results in Eclipse's console window.

### 2.3 Intellij
[Intellij](https://www.jetbrains.com/idea/) is another graphical software development environment that tends to be more user-friendly.

#### Setting the Lab Up in Intellij
* Download `acmdb-lab0.tar.gz` to anywhere you like and umcompress it.
* Once Intellij is installed, start it, select "Import Project" and navigate to the `acmdb-lab0` directory. 
Choose "Import Project from external model" -> "Eclipse".
* Click "Next" until you need to select the project SDK. Make sure you select "JDK 1.8" or "openjdk-8".
* Click "Finish",  and you should be able to see "acmdb-lab0" as a new project in the Project Explorer tab on the left-hand side of your screen. Opening this project reveals the directory structure discussed above - implementation code can be found in "src," and unit tests and system tests found in "test."
* Select "File" -> "Project Structure". In the project setting page, click "Libraries" -> "+"(New project library) -> "Java", and select the "lib" directory contained in the "acmdb-lab0" directory. This step will add necessary libraries to the project.

#### Running Individual Unit and System Tests

To run a unit test or system test (both are JUnit tests, and can be initialized the same way), go to the Package Explorer tab on the left side of your screen. Under the "acmdb-lab0" project, open the "test" directory. Unit tests are found in the "simpledb" package, and system tests are found in the "simpledb.systemtests" package. To run one of these tests, select the test (they are all called *Test.java - don't select TestUtil.java or SystemTestUtil.java), right click on it, select "Run '*Test.java' ". This will bring up a JUnit tab, which will tell you the status of the individual tests within the JUnit test suite, and will show you exceptions and other errors that will help you debug problems.

#### Running Ant Build Targets

If you want to run commands such as "ant test" or "ant systemtest," find the ant tool at the top-right corner, open the ant tool bar and choose "+". Then you can add the "build.xml" in the "acmdb-lab0" directory to the ant tool and see a set of running targets. Double click the targets you want to run (probably "dist" or "test" or "systemtest"), and you will see the results in Intellij's console window.

## 3. Submission
Good work! You have successfully tested the ant tool. Now we will focus on how to submit your assignment.

You must submit your code (see below) as well as a short (2 pages, maximum) writeup describing your experience. This writeup should:

* Describe how long you spent on the lab, and whether there was anything you found particularly difficult or confusing.

### 3.1 Submit Process
To submit your code, please follow the guide below:
* Create a github repo for your project, name it as follows: `acmdb20-"student-id"`, where "student-id" refers to your student number. For example, my student number is 516030910594, so the project name will be `acmdb20-516030910594`. 
* Add `acmdb-lab0` as a directory to your github repo. 
* Add  `azure-pipelines.yml` to your github repo. 
* Add TAs github account to your repo. 
* Place the write-up in a file called answers.txt or answers.pdf in the top level of your acmdb-lab0 directory.
* Stop editing your lab0 folder (and commit all your changes) before the due date and time. You may submit (commit) your code multiple times; we will use the latest version you submit that arrives before the deadline. 

TA1: [Zihao Xu](https://github.com/shsjxzh)

TA2: [Chang Liu](https://github.com/only-changer)

**Note:** We recommend you to make your github repo private.

## 4. Test on Grading Environment 
Before testing, we will replace your `build.xml` and the entire contents of the test directory with our version of these files. The whole grading pipeline will look roughly like this:
```console
[start linux docker image]
[clone your repo]
$ cd ./acmdb-lab0
[replace build.xml and all the tests]
$ ant test
$ ant systemtest
```
Since you may use system other than linux to develop the code and only test it locally, it is highly possible that it will fail on the grading machine. Therefore, we recommend you to test it by [Azure pipelines](https://azure.microsoft.com/en-us/services/devops/pipelines/). 

### 4.1 Azure pipelines test
Azure pipelines is a system that can continuously build and test your code on any platform. It severs as an "online virtual machine" to verify your code on machines with the same configuration of our grading system. We have included an `azure-pipelines.yml` file in `acmdb-lab0.tar.gz`, which encodes our grading machine environment. You can try the system following this [guide](https://www.cnblogs.com/tylerzhou/p/9806814.html).

## 5. Submitting a bug
Please submit (friendly!) bug reports to both TAs. When you do, please try to include:
* A description of the bug.
* A `.java` file we can drop in the `test/simpledb` directory, compile, and run.
* A `.txt` file with the data that reproduces the bug. We should be able to convert it to a `.dat` file using HeapFileEncoder.

## 6. Grading
This lab will not be graded.

## 7. Conclusion
Congratulations! You have finished all the tasks in lab 0, and now you are well prepared for the following labs~ We've had a lot of fun designing this assignment, and we hope you enjoy hacking on it!



