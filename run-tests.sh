#!/bin/bash
docker run --name impala -d codingtony/impala
sleep 20

export IMPALA_HOST=localhost
export IMPALA_PORT=21000

dotnet restore -s https://www.myget.org/F/s2/ -s https://api.nuget.org/v3/index.json
dotnet build
dotnet publish

pushd ImpalaSupplyCollectorLoader/bin/Debug/netcoreapp2.2/publish
dotnet SupplyCollectorDataLoader.dll -xunit ImpalaSupplyCollector impala://$IMPALA_HOST:$IMPALA_PORT
popd

dotnet test

docker stop impala
docker rm impala
