# LRBA-MX-JSPRK-EXAMEN-V00

## JOB DESCRIPTION

...Complete with the description of your job...


## PROJECT STRUCTURE

```java
- C:\Users\SoporteTI\Documents\LRBA\LRBA-MX-JSPRK-EXAMEN-V00
| - local-execution
  | - database
    | - logicalDataStore.json
    | - physicalDataStore.json
  | - files
    | - input1.csv
  | - deploymentConfig.properties.required
  | - inputParameters.properties.required
| - src
  | - main
    | - java
      | - com
        | - bbva
          | - lrba
            | - mx
              | - jsprk
                | - examen
                  | - v00
                    | - JobExamenBuilder.java
                    | - Transformer.java
| - build.json
| - README.md
```

### DESCRIPTION PROJECT STRUCTURE

- Folder **local-execution** contains the information necessary to run the job locally:
  - **database** folder: Contains the files associated to the logical and physical data store.
    - Logical data store (**logicalDataStore.json**) contains:
      - Data source type.
      - Physical field indicates the physical data store file name.
      - Service field used to search in the physical data store the entry with the corresponding service name.
    - Physical data store (**physicalDataStore.json**) contains:
      - Service name that matches with the logical data store service field.
      - URL where is possible performing read or write operations.
  - **files** folder: Contains the files used in the generated sourceBuilder and the output of the execution.
  - **deploymentConfig.properties.required** & **inputParameters.properties.required**. The developer must put
    all the mandatory deployment configuration variables and input parameters necessary to run the application correctly.

- Folder **src** contains the java files:
  - **JobExamenBuilder.java**: Indicates the sources and targets the generated application
   will be able to read and write.
  - **Transformer.java**: Class to operate with datasets.

- **build.json** file: Necessary for the architecture to deploy the job and it can not be modified by the developer.