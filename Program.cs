using TypeDB.Driver.Api;
using TypeDB.Driver.Common;

class WelcomeToTypeDB
{
    static void Main(string[] args)
    {
        string dbName = "access-management-db";
        string serverAddr = "127.0.0.1:1729";

        try
        {
            using (ITypeDBDriver driver = TypeDB.Driver.Drivers.CoreDriver(serverAddr))
            {
                driver.Databases.Create(dbName);
                IDatabase database = driver.Databases.Get(dbName);

                // Example of one transaction for one session
                using (ITypeDBSession session = driver.Session(dbName, SessionType.Schema))
                {
                    // Example of multiple queries for one transaction
                    using (ITypeDBTransaction transaction = session.Transaction(TransactionType.Write))
                    {
                        transaction.Query.Define("define person sub entity;").Resolve();

                        string longQuery = "define name sub attribute, value string; person owns name;";
                        transaction.Query.Define(longQuery).Resolve();

                        transaction.Commit();
                    }
                }

                // Example of multiple transactions for one session
                using (ITypeDBSession session = driver.Session(dbName, SessionType.Data))
                {
                    // Examples of one query for one transaction
                    using (ITypeDBTransaction transaction = session.Transaction(TransactionType.Write))
                    {
                        string query = "insert $p isa person, has name 'Alice';";
                        IEnumerable<IConceptMap> insertResults = transaction.Query.Insert(query);

                        Console.WriteLine($"Inserted with {insertResults.Count()} result(s)");

                        transaction.Commit();
                    }

                    using (ITypeDBTransaction transaction = session.Transaction(TransactionType.Write))
                    {
                        IEnumerable<IConceptMap> insertResults =
                            transaction.Query.Insert("insert $p isa person, has name 'Bob';");

                        foreach (IConceptMap insertResult in insertResults)
                        {
                            Console.WriteLine($"Inserted: {insertResult}");
                        }

                        // transaction.Commit(); // Not committed
                    }

                    using (ITypeDBTransaction transaction = session.Transaction(TransactionType.Read))
                    {
                        IConceptMap[] matchResults =
                            transaction.Query.Get("match $p isa person, has name $n; get $n;").ToArray();

                        // Matches only Alice as Bob has not been committed
                        var resultName = matchResults[0].Get("n");
                        Console.WriteLine($"Found the first name: {resultName.AsAttribute().Value.AsString()}");

                        if (matchResults.Length > 1) // Will work only if the previous transaction is committed
                        {
                            Console.WriteLine($"Found the second name as concept: {matchResults[1]}");
                        }
                    }
                }

                database.Delete();
            }
        }
        catch (TypeDBDriverException e)
        {
            Console.WriteLine($"Caught TypeDB Driver Exception: {e}");
            // ...
        }
    }
}
