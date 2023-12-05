# AFProbe
## Description
A sample project to demonstrate how AF SDK is used by Seeq.
## Requirements
* Should be self contained
* Should be able to control the level of parallelism
* Should be able to control what attributes are consumed

## Build

First we may need to get the dependencies. This step is only needed if your IDE is not getting them automatically. 
This can be done by running the following command in the root of the project:
```commandline
nuget install packages.config -o packages
```
Then you have to add the reference to the AF SDK dlls. This has to be done manually.

## Usage
```commandline
AFProbe.exe
```
## Command line arguments
```commandline
Arguments:
  -a, --af_server_id=VALUE   AF Server ID.
  -u, --username=VALUE       Optional username.
  -p, --password=VALUE       Optional password.
  -s, --start=VALUE          Request start (ISO time. It defaults to 2019-01-02T00:00:00.0Z).
  -e, --end=VALUE            Request end (ISO time). It defaults to 2023-11-02T00:00:00.0Z
  -c, --samples_per_chunk=VALUE
                             Maximum number of samples to get in a chunk. It default to 5000000
  -t, --threads=VALUE        Max degree of parallelism.
  -f, --file=VALUE           File from where to read the data IDs (one per line).
```

## Examples

### Example 1: 64 parallel requests with 32768 samples per chunk
Get all samples in the default time range for all attributes listed in the csv file, making requests on 64 threads and getting 32768 samples per chunk.
```AFProbe.exe -u <yourUser> -p <yourPassword> -a <AF server GUID> -f c:\full path to csv file\the file.csv -t 64 -c 32768```

### Example 2: 64 parallel requests with ARCMaxCollector samples per chunk
Get all samples in the default time range for all attributes listed in the csv file, making requests on 64 threads 
and getting the maximum number of samples (automatically calculated based on AcrMaxCollector) per chunk.
```AFProbe.exe -u <yourUser> -p <yourPassword> -a <AF server GUID> -f c:\full path to csv file\the file.csv -t 64```

### Example 3: serial requests with ARCMaxCollector samples per chunk  
Get all samples in the default time range for all attributes listed in the csv file, making all requests on one single thread
and getting the maximum number of samples (automatically calculated based on AcrMaxCollector) per chunk.
```AFProbe.exe -u <yourUser> -p <yourPassword> -a <AF server GUID> -f c:\full path to csv file\the file.csv -t 1```

### Example csv file
The csv file should contain one ID per line. The ID is the concatenation of the 
element ID and the attribute ID, separated by a slash.
The csv file should contain no header.

```csv
1a273028-e26e-11ed-9323-0022488c984f/03f091f1-0a88-51b9-2c70-ea402daa69ca
f59510e1-b4be-11ea-810b-000d3a701f3d/ec42b138-5c58-51be-3e58-ea6f5f56eeb8
```