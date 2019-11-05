using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using ImpalaSharp;
using ImpalaSharp.Thrift;
using Microsoft.AspNetCore.Server.Kestrel.Internal.Http;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;

namespace ImpalaSupplyCollector
{
    public class ImpalaSupplyCollector : SupplyCollectorBase
    {
        public const string PREFIX = "impala://";

        public string BuildConnectionString(string host, int port)
        {
            return $"{PREFIX}{host}:{port}";
        }

        public override List<string> DataStoreTypes()
        {
            return (new[] { "Impala" }).ToList();
        }

        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize)
        {
            var results = new List<string>();

            using (var conn = Connect(dataEntity.Container.ConnectionString)) {
                var result =
                    conn.Query(
                        $"select {dataEntity.Name} from {dataEntity.Collection.Name} limit {sampleSize}");

                foreach (var row in result.Result) {
                    var columnName = row.Keys.First();
                    results.Add(row[columnName].ToString());
                }
            }

            return results;
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container)
        {
            var metrics = new List<DataCollectionMetrics>();

            using (var conn = Connect(container.ConnectionString)) {
                var result = conn.Query("show tables");
                foreach (var row in result.Result) {
                    var columnName = row.Keys.First();
                    metrics.Add(new DataCollectionMetrics() { Name = row[columnName].ToString() });
                }

                foreach (var metric in metrics)
                {
                    try {
                        conn.Execute($"compute stats default.{metric.Name}");
                    } catch(Exception) { /* ignore */}

                    result = conn.Query($"show table stats {metric.Name}");

                    metric.RowCount = 0;
                    metric.TotalSpaceKB = 0;

                    foreach (var row in result.Result)
                    {
                        foreach (var rowKey in row.Keys)
                        {
                            if ("#Rows".Equals(rowKey))
                            {
                                metric.RowCount += Int64.Parse(row[rowKey]);
                            }
                            else if ("Size".Equals(rowKey))
                            {
                                var sizeStr = row[rowKey];
                                if (sizeStr.EndsWith("TB"))
                                {
                                    metric.TotalSpaceKB +=
                                        Int64.Parse(sizeStr.Substring(0, sizeStr.Length - 2)) * 1024 * 1024 * 1024;
                                }
                                else if (sizeStr.EndsWith("GB"))
                                {
                                    metric.TotalSpaceKB +=
                                        Int64.Parse(sizeStr.Substring(0, sizeStr.Length - 2)) * 1024 * 1024;
                                }
                                else if (sizeStr.EndsWith("MB"))
                                {
                                    metric.TotalSpaceKB +=
                                        Int64.Parse(sizeStr.Substring(0, sizeStr.Length - 2)) * 1024;
                                }
                                else if (sizeStr.EndsWith("KB"))
                                {
                                    metric.TotalSpaceKB +=
                                        Int64.Parse(sizeStr.Substring(0, sizeStr.Length - 2));
                                }
                                else if (sizeStr.EndsWith("B"))
                                {
                                    metric.TotalSpaceKB +=
                                        Int64.Parse(sizeStr.Substring(0, sizeStr.Length - 1)) / 1024;
                                }
                                else
                                {
                                    metric.TotalSpaceKB += Int64.Parse(sizeStr);
                                }
                            }
                        }
                    }

                    metric.UsedSpaceKB = metric.TotalSpaceKB;
                }
            }

            return metrics;
        }

        private DataType ConvertDataType(string dbDataType)
        {
            if ("bigint".Equals(dbDataType))
            {
                return DataType.Decimal;
            }
            else if ("boolean".Equals(dbDataType))
            {
                return DataType.Boolean;
            }
            else if ("int".Equals(dbDataType))
            {
                return DataType.Long;
            }
            else if ("smallint".Equals(dbDataType))
            {
                return DataType.Short;
            }
            else if ("decimal".Equals(dbDataType))
            {
                return DataType.Decimal;
            }
            else if ("tinyint".Equals(dbDataType))
            {
                return DataType.Byte;
            }
            else if ("char".Equals(dbDataType))
            {
                return DataType.Char;
            }
            else if ("varchar".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("string".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("ntext".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("float".Equals(dbDataType))
            {
                return DataType.Double;
            }
            else if ("double".Equals(dbDataType))
            {
                return DataType.Double;
            }
            else if ("date".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("timestamp".Equals(dbDataType))
            {
                return DataType.DateTime;
            } else if (dbDataType.StartsWith("char(")) {
                return DataType.String;
            } else if (dbDataType.StartsWith("decimal(")) {
                return DataType.Decimal;
            }

            return DataType.Unknown;
        }


        public override (List<DataCollection>, List<DataEntity>) GetSchema(DataContainer container)
        {
            var collections = new List<DataCollection>();
            var entities = new List<DataEntity>();

            using (var conn = Connect(container.ConnectionString))
            {
                var result = conn.Query("show tables");
                foreach (var row in result.Result)
                {
                    var columnName = row.Keys.First();
                    collections.Add(new DataCollection(container, row[columnName]));
                }

                foreach (var collection in collections)
                {
                    result = conn.Query($"describe {collection.Name}");

                    foreach (var row in result.Result)
                    {
                        var columnName = row["name"];
                        var dataType = row["type"];

                        if (dataType.StartsWith("struct<") || dataType.StartsWith("array<") || dataType.StartsWith("map<"))
                        {
                            var definition =
                                dataType.Substring("struct<".Length, dataType.Length - "struct<".Length - 1);

                            var fieldPairs = definition.Split(",");
                            foreach (var fieldPair in fieldPairs)
                            {
                                var nametype = fieldPair.Split(":");

                                entities.Add(new DataEntity(columnName + "." + nametype[0], ConvertDataType(nametype[1]), nametype[1], container, collection));
                            }
                        }
                        else
                        {
                            entities.Add(new DataEntity(columnName, ConvertDataType(dataType), dataType, container, collection));
                        }
                    }
                }
            }

            return (collections, entities);
        }

        private ImpalaClient Connect(string connectString)
        {
            if (!connectString.StartsWith(PREFIX))
                throw new ArgumentException("Invalid connection string!");

            var hostPort = connectString.Substring(PREFIX.Length).Split(":");
            var host = hostPort[0];
            var port = Int32.Parse(hostPort[1]);

            return ImpalaClient.Connect(host, port);
        }

        public override bool TestConnection(DataContainer container)
        {
            try
            {
                using (var conn = Connect(container.ConnectionString))
                {
                    conn.Query("show tables");
                }

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

    }
}
