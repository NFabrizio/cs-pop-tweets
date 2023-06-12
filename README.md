# pop-tweets

A real-time streaming data analytics system using Apache Storm. It detects the most frequently
occurring hash tags from the live Twitter data stream in real-time.

_Authors_: [Chen Yuan](https://github.com/yc47084613), [Nick Fabrizio](https://github.com/NFabrizio)

Project is written in Java, and uses Apache Storm for data stream processing.

## Usage
This project is intended to be submitted to an Apache Storm cluster as a package jar with dependencies.  

* This project uses Twitter4J, so proper configuration is required.  
  * Add a directory named conf to the root of the project.
  * Inside the /conf directory, add a twitter4j.properties file with the basic configuration values described at https://twitter4j.org/en/configuration.html.
  * Inside the /conf directory, add a log4j.properties file with a basic Log4J configuration.
* Run clean, install and build using Maven for the project.  
  **Note: Be sure to build a jar with dependencies in order to submit to an Apache Storm cluster.**
* To submit the jar with dependencies to the Apache Storm cluster and have it run without parallelization, run the following command:  
    * `storm jar path/to/pop-tweets-jar-with-dependencies.jar main.java.org.poptweets.PopTweetsTopology /absolute/path/to/logfile.txt [epsilon_value] [threshold_value]`  
    Example command usage:  
    `storm jar path/to/pop-tweets-1.0-SNAPSHOT-jar-with-dependencies.jar main.java.org.poptweets.PopTweetsTopology /absolute/path/to/TwitterLogs.txt 0.5 0.6`  
* To submit the jar with dependencies to the Apache Storm cluster and have it run with parallelization, run the following command:  
    * `storm jar path/to/pop-tweets-jar-with-dependencies.jar main.java.org.poptweets.PopTweetsTopology /absolute/path/to/logfile.txt [epsilon_value] [threshold_value] [parallel_value]`  
    Example command usage:  
    `storm jar path/to/pop-tweets-1.0-SNAPSHOT-jar-with-dependencies.jar main.java.org.poptweets.PopTweetsTopology /absolute/path/to/TwitterLogs.txt 0.5 0.6 4`
