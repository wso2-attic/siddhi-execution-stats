# siddhi-execution-stats
======================================
---
##### New version of Siddhi v4.0.0 is built in Java 8.
##### Latest Released Version v4.0.0-m15.

This extension provides statistical functions for aggregated events. Currently this contains median 
function which calculate the median of aggregated events. Calculation of median is done for 
each event arrival and expiry, it is not recommended to use this extension for large window sizes.

Features Supported
------------------
This function returns the median of aggregated events.

 - data : Data value which can be float,int, double or long.
 
Prerequisites for using the feature
------------------
  - Siddhi Stream should be defined
  
Deploying the feature
------------------
   Feature can be deploy as a OSGI bundle by putting jar file of component to DAS_HOME/lib directory of DAS 4.0.0 pack. 
   
Example Siddhi Queries
------------------
      - from inputStream#window.length(5)
        select stats:median(value) as medianOfValues
        insert into outputStream;
   
How to Contribute
------------------
   * Send your bug fixes pull requests to [master branch] (https://github.com/wso2-extensions/siddhi-execution-stats/tree/master) 
   
Contact us 
------------------
   Siddhi developers can be contacted via the mailing lists:
     * Carbon Developers List : dev@wso2.org
     * Carbon Architecture List : architecture@wso2.org
   
We welcome your feedback and contribution.
------------------

## API Docs:

1. <a href="./api/1.0.2-SNAPSHOT">1.0.2-SNAPSHOT</a>
