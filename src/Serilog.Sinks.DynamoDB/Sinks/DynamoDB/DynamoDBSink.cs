// Copyright 2015 Serilog Contributors
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.DynamoDB
{
  public class DynamoDBSink : PeriodicBatchingSink
  {
    private readonly IFormatProvider _formatProvider;
    private readonly string _tableName;

    public DynamoDBSink(IFormatProvider formatProvider, string tableName) :base(1000, TimeSpan.FromSeconds(15))
    {
      _formatProvider = formatProvider;
      _tableName = tableName;
      AmazonDynamoDbConfig = new AmazonDynamoDBConfig();
      OperationConfig = new DynamoDBOperationConfig {OverrideTableName = tableName};
    }

    private DynamoDBOperationConfig OperationConfig { get; set; }

    private AmazonDynamoDBConfig AmazonDynamoDbConfig { get; set; }

    protected override async Task EmitBatchAsync(IEnumerable<LogEvent> events)
    {
      var records = events.Select(x => new LogDocument(x, x.RenderMessage(_formatProvider)));

      try
      {
        using (var client = new AmazonDynamoDBClient(AmazonDynamoDbConfig))
        {
          using (var context = new DynamoDBContext(client))
          {
            var batchWrite = context.CreateBatchWrite<LogDocument>(OperationConfig);
            batchWrite.AddPutItems(records);
            await batchWrite.ExecuteAsync();
          }
        }
      }
      catch (Exception exception)
      {
        SelfLog.WriteLine("Unable to write events to DynamoDB Sink for {0}: {1}", _tableName, exception);
      }
    }
  }
}