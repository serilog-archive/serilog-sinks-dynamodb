using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Serilog.Generator.Actors;
using Serilog.Generator.Model;

namespace Serilog.Sinks.DynamoDB.Sample
{
  class Program
  {
    static void Main(string[] args)
    {
      Log.Logger = new LoggerConfiguration()
           .WriteTo.LiterateConsole()
           .WriteTo.DynamoDB("Log-Dev")
           .Enrich.WithThreadId()
           .Enrich.WithProperty("Serilog.Sinks.DynamoDB.Sample", "Sample App")
           .MinimumLevel.Debug()
           .CreateLogger();


      Log.Information("Testing Dynamo DB");
      Log.Error(new AbandonedMutexException("Hello"), "An exception from the Serilog DynamoDB Sample");

      const int initialCustomers = 1;
      var catalog = new Catalog();

      var customers = new ConcurrentBag<Customer>(Enumerable.Range(0, initialCustomers)
          .Select(_ => new Customer(catalog)));

      var traffic = new TrafficReferral(customers, catalog);
      var admin = new Administrator(catalog);

      foreach (var c in customers)
          c.Start();

      admin.Start();
      traffic.Start();

      Console.ReadLine();
    }
  }
}
