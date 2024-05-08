# Sport Data Analysis Application

With this application we can analyse Sports Data given in a CSV format. And the application can process the data and provides results e.g.
- Match Details
- League Table
- Game Score
- Players Statistics

## Technology
The application is able to parse the CSV data with following Tech :
- Java 17
- Scala 2.13.14
- Apache Spark 3.5.1

## Project Structure
The raw CSV data is stored at ![Raw Data](/data/Dataset%202rounds%20Eredivie%2020172018.csv)
The scala code is placed at `src/main/scala/*`
The images directory holds the images used for this README file



## How to run the Application
There are following options to run the application
- #### With Intellij : 
  1. First you have add a VM argument in Intellij Run configuration. This is needed as scala still uses jdk internal modules which are now restricted since jdk 9. 
    ![VM Options](/images/vm-options.png)
  2. Then, You can run the main method at `com.sports.data.analysis.Runner`
- #### With Command line Terminal 
  - Run the following commands in order
    1. `export JAVA_OPTS='--add-exports java.base/sun.nio.ch=ALL-UNNAMED'` 
    2. `sbt test`
    3. And you will see following test results in your terminal
       ![Test Results](/images/test-results.png)
