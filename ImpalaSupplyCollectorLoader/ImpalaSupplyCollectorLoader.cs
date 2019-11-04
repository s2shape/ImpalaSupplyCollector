using System;
using System.IO;
using System.Text;
using ImpalaSharp;
using S2.BlackSwan.SupplyCollector.Models;
using SupplyCollectorDataLoader;

namespace ImpalaSupplyCollectorLoader
{
    public class ImpalaSupplyCollectorLoader : SupplyCollectorDataLoaderBase
    {
        public const string PREFIX = "impala://";

        private ImpalaClient Connect(string connectString)
        {
            if (!connectString.StartsWith(PREFIX))
                throw new ArgumentException("Invalid connection string!");

            var hostPort = connectString.Substring(PREFIX.Length).Split(":");
            var host = hostPort[0];
            var port = Int32.Parse(hostPort[1]);

            return ImpalaClient.Connect(host, port);
        }

        public override void InitializeDatabase(DataContainer dataContainer)
        {

        }

        public override void LoadSamples(DataEntity[] dataEntities, long count)
        {
            using (var conn = Connect(dataEntities[0].Container.ConnectionString))
            {
                var sb = new StringBuilder();
                sb.Append("CREATE TABLE ");
                sb.Append(dataEntities[0].Collection.Name);
                sb.Append(" (\n");
                sb.Append("id_field int");

                foreach (var dataEntity in dataEntities)
                {
                    sb.Append(",\n");
                    sb.Append(dataEntity.Name.ToLower());
                    sb.Append(" ");

                    switch (dataEntity.DataType)
                    {
                        case DataType.String:
                            sb.Append("string");
                            break;
                        case DataType.Int:
                            sb.Append("int");
                            break;
                        case DataType.Double:
                            sb.Append("double");
                            break;
                        case DataType.Boolean:
                            sb.Append("boolean");
                            break;
                        case DataType.DateTime:
                            sb.Append("timestamp");
                            break;
                        default:
                            sb.Append("int");
                            break;
                    }

                    sb.AppendLine();
                }

                sb.Append(")");
                conn.Query(sb.ToString());

                var r = new Random();
                long rows = 0;
                while (rows < count)
                {
                    long bulkSize = 10000;
                    if (bulkSize + rows > count)
                        bulkSize = count - rows;

                    sb = new StringBuilder();
                    sb.Append("INSERT INTO ");
                    sb.Append(dataEntities[0].Collection.Name);
                    sb.Append("( id_field");

                    foreach (var dataEntity in dataEntities)
                    {
                        sb.Append(", ");
                        sb.Append(dataEntity.Name.ToLower());
                    }
                    sb.Append(") VALUES ");

                    for (int i = 0; i < bulkSize; i++)
                    {
                        if (i > 0)
                            sb.Append(", ");

                        sb.Append("(");
                        sb.Append(rows + i);
                        foreach (var dataEntity in dataEntities)
                        {
                            sb.Append(", ");

                            switch (dataEntity.DataType)
                            {
                                case DataType.String:
                                    sb.Append("'");
                                    sb.Append(new Guid().ToString());
                                    sb.Append("'");
                                    break;
                                case DataType.Int:
                                    sb.Append(r.Next().ToString());
                                    break;
                                case DataType.Double:
                                    sb.Append(r.NextDouble().ToString().Replace(",", "."));
                                    break;
                                case DataType.Boolean:
                                    sb.Append(r.Next(100) > 50 ? "true" : "false");
                                    break;
                                case DataType.DateTime:
                                    var val = DateTimeOffset
                                        .FromUnixTimeMilliseconds(
                                            DateTimeOffset.Now.ToUnixTimeMilliseconds() + r.Next()).DateTime;
                                    sb.Append("'");
                                    sb.Append(val.ToString("s"));
                                    sb.Append("'");
                                    break;
                                default:
                                    sb.Append(r.Next().ToString());
                                    break;
                            }
                        }

                        sb.Append(")");
                    }

                    conn.Query(sb.ToString());

                    rows += bulkSize;
                    Console.Write(".");
                }

                Console.WriteLine();
            }
        }

        public override void LoadUnitTestData(DataContainer dataContainer)
        {
            using (var conn = Connect(dataContainer.ConnectionString))
            {
                using (var reader = new StreamReader("tests/data.sql"))
                {
                    var sb = new StringBuilder();
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        if (String.IsNullOrEmpty(line))
                            continue;

                        sb.AppendLine(line);
                        if (line.TrimEnd().EndsWith(";"))
                        {
                            Console.WriteLine(sb.ToString());
                            conn.Query(sb.ToString().TrimEnd(new[] { '\n', '\r', '\t', ' ', ';' }));
                            sb.Clear();
                        }
                    }
                }
            }
        }

    }
}
