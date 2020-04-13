using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using System.IO;
using System.Text;
using Microsoft.SqlServer.Server;
using System.Data.SqlTypes;
using System;

public class TestProcAws
{
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void PublishDataToKinesis(SqlString sqlData,
                                        SqlString sqlPartitionKey, SqlString sqlKinesisStreamName,
                                        SqlString sqlAccessKeyId, [SqlFacet(MaxSize = -1)] SqlString sqlSecretAccessKey,
                                        SqlString sqlRegionalEndPoint,
                                        out SqlString result, out SqlString exception)
    {
   
            var data =sqlData.ToString().Trim();
            var partitionKey = sqlPartitionKey.ToString().Trim();
            var kinesisStreamName = sqlKinesisStreamName.ToString().Trim();
            var accessKeyId = sqlAccessKeyId.ToString().Trim();
            var secretAccessKey = sqlSecretAccessKey.ToString().Trim();
            var regionalEndPoint = sqlRegionalEndPoint.ToString().Trim();
            exception = new SqlString("");

            // note: this constructor relies on you having set up a credential profile
            // named 'default', or have set credentials in environment variables
            // AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY, or have an application settings
            // file. See https://docs.aws.amazon.com/sdk-for-net/v3/developer-guide/net-dg-config-creds.html
            // for more details and other constructor options.
            //var kinesisClient = new AmazonKinesisClient(_regionEndpoint);

            var kinesisClient = new AmazonKinesisClient(accessKeyId, secretAccessKey, Amazon.RegionEndpoint.GetBySystemName(regionalEndPoint));
            try
            {
                var requestRecord = new PutRecordRequest
                {
                    StreamName = kinesisStreamName,
                    PartitionKey = partitionKey,
                    Data = new MemoryStream(Encoding.UTF8.GetBytes(data))
                };

                var responseRecord = kinesisClient.PutRecord(requestRecord);
                SqlContext.Pipe.Send($"Successfully published. Record:{data}, Seq:{responseRecord.SequenceNumber}" + Environment.NewLine);

                result = responseRecord.HttpStatusCode.ToString();
            }
            catch (Exception ex)
            {

                SqlContext.Pipe.Send(($"Failed to publish. Exception: {ex.Message}") + Environment.NewLine);
                int maxLength = 50;
                exception = new SqlString(ex.GetType().ToString().Length <=maxLength? ex.GetType().ToString(): ex.GetType().ToString().Remove(maxLength));
                result = "fail";
            }
        }
    }
