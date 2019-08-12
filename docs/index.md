siddhi-execution-stats
======================================

The **siddhi-execution-stats extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that provides statistical functions for aggregated events. Currently this contains median 
function which calculate the median of aggregated events. Calculation of median is done for  each event arrival and expiry, it is not recommended to use this extension for large window sizes.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-stats">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-stats/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-stats/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-stats/api/2.0.0">2.0.0</a>.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-stats/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.execution.stats</groupId>
        <artifactId>siddhi-execution-stats</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/view/All%20Builds/job/siddhi/job/siddhi-execution-stats/badge/icon)](https://wso2.org/jenkins/view/All%20Builds/job/siddhi/job/siddhi-execution-stats/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-stats/api/2.0.0/#median-aggregate-function">median</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#aggregate-function">Aggregate Function</a>)*<br> <div style="padding-left: 1em;"><p>This extension returns the median of aggregated events.<br>&nbsp;As the calculation of the median is performed with the arrival and expiry of each event, it is not recommended to use this extension for large window sizes.</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-stats/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-stats/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our stats approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this stats opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
