# ImpalaSupplyCollector
A supply collector designed to connect to Apache Impala

## Building

Run
```
dotnet restore -s https://www.myget.org/F/s2/ -s https://api.nuget.org/v3/index.json
dotnet build
```

## Testing
Use `run-tests.sh`

## Known issues
* No support for complex types - couldn't find working docker image for latest Impala
* Random sampling doesn't work - sample() method doesn't work with this Impala version
* There's a problem with metrics - must call "COMPUTE STATS" after creating table or before running `show table stats`, but it's working only from impala-shell.
Returns "cannot match input" error



