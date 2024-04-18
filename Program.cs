// tag::code[]
// tag::import[]
using System;
using System.IO;
using System.Collections.Generic;
using TypeDB.Driver.Api;
using TypeDB.Driver.Common;
using Newtonsoft.Json.Linq;

class WelcomeToTypeDB
{
// end::import[]
// tag::constants[]
    const string DB_NAME = "sample_app_db";
    const string SERVER_ADDR = "127.0.0.1:1729";
    enum Edition { Core, Cloud }
    const Edition TYPEDB_EDITION = Edition.Core;
    const string CLOUD_USERNAME = "admin";
    const string CLOUD_PASSWORD = "password";
    // end::constants[]
    // tag::db-schema-setup[]
    void DbSchemaSetup(IDatabase database, string schemaFile = "iam-schema.tql") {
        string defineQuery = File.ReadAllText(schemaFile);
        using (ITypeDBSession session = database.Session(SessionType.Schema)) {
            using (ITypeDBTransaction transaction = session.Transaction(TransactionType.Write)) {
                Console.WriteLine("Defining schema...");
                transaction.Query.Define(defineQuery).Resolve();
                transaction.Commit();
                Console.WriteLine("OK");
            }
        }
    }
    // end::db-schema-setup[]
    // tag::db-dataset-setup[]
    void DbDatasetSetup(IDatabase database, string dataFile = "iam-data-single-query.tql") {
        string insertQuery = File.ReadAllText(dataFile);
        using (ITypeDBSession session = database.Session(SessionType.Data)) {
            using (ITypeDBTransaction transaction = session.Transaction(TransactionType.Write)) {
                Console.WriteLine("Loading data...");
                IEnumerable<IConceptMap> response = transaction.Query.Insert(insertQuery);
                int count = response.Count();
                transaction.Commit();
                Console.WriteLine("OK");
            }
        }
    }
    // end::db-dataset-setup[]
    // tag::create_new_db[]
    bool CreateDatabase(ITypeDBDriver driver, string dbName) {
        Console.WriteLine("Creating a new database...");
        driver.Databases.Create(dbName);
        Console.WriteLine("OK");
        IDatabase database = driver.Databases.Get(dbName);
        DbSchemaSetup(database);
        DbDatasetSetup(database);
        return true;
    }
    // end::create_new_db[]
    // tag::replace_db[]
    bool ReplaceDatabase(ITypeDBDriver driver, string dbName) {
        Console.WriteLine("Deleting an existing database...");
        driver.Databases.Get(dbName).Delete();
        Console.WriteLine("OK");
        return CreateDatabase(driver, dbName);
    }
    // end::replace_db[]
    // tag::test-db[]
    bool DbCheck(IDatabase database) {
        using (ITypeDBSession session = database.Session(SessionType.Data)) {
            using (ITypeDBTransaction transaction = session.Transaction(TransactionType.Read)) {
                Console.WriteLine("Testing the database...");
                string testQuery = "match $u isa user; get $u; count;";
                long result = transaction.Query.Aggregate(testQuery).As<long>();
                if (result == 3) {
                    Console.WriteLine("Passed");
                    return true;
                } else {
                    Console.WriteLine($"Failed with the result: {result}\nExpected result: 3.");
                    return false;
                }
            }
        }
    }
    // end::test-db[]
    // tag::db-setup[]
    bool DbSetup(ITypeDBDriver driver, string dbName, bool dbReset = false) {
        Console.WriteLine($"Setting up the database: {dbName}");
        if (driver.Databases.Contains(dbName)) {
            if (dbReset || ConsoleYesNoPrompt("Found a pre-existing database. Do you want to replace it? (Y/N)")) {
                return ReplaceDatabase(driver, dbName);
            } else {
                Console.WriteLine("Reusing an existing database.");
            }
        } else {
            return CreateDatabase(driver, dbName);
        }
        IDatabase database = driver.Databases.Get(dbName);
        return DbCheck(database);
    }
    // end::db-setup[]
    // tag::json[]
    void PrintJSON(JObject json) {
        if (json.IsString) {
            Console.WriteLine($"'{json.AsString()}'");
        }
        if (json.IsMap) {
            foreach (var p in json.AsMap()) {
                Console.WriteLine($"{p.Key}:");
                PrintJSON(p.Value);
            }
        }
    }
    // end::json[]
    // tag::fetch[]
    List<JObject> FetchAllUsers(IDatabase database) {
        List<JObject> users = new List<JObject>();
        using (ITypeDBSession session = database.Session(SessionType.Data)) {
            using (ITypeDBTransaction transaction = session.Transaction(TransactionType.Read)) {
                IEnumerable<JObject> queryResult = transaction.Query.FetchJson("match $u isa user; fetch $u: full-name, email;");
                int c = 1;
                foreach (JObject user in queryResult) {
                    users.Add(user);
                    Console.WriteLine($"User #{c++} ");
                    PrintJSON(user);
                    Console.WriteLine();
                }
            }
        }
        return users;
    }
    // end::fetch[]
    // tag::insert[]
    IEnumerable<IConceptMap> InsertNewUser(IDatabase database, string name, string email) {
        List<IConceptMap> response = new List<IConceptMap>();
        using (ITypeDBSession session = database.Session(SessionType.Data)) {
            using (ITypeDBTransaction transaction = session.Transaction(TransactionType.Write)) {
                string query = $"insert $p isa person, has full-name {name}, has email {email};";
                IEnumerable<IConceptMap> result = transaction.Query.Insert(query);
                foreach (IConceptMap conceptMap in result) {
                    string retrievedName = conceptMap.Get("fn").AsAttribute().Value.AsString();
                    string retrievedEmail = conceptMap.Get("e").AsAttribute().Value.AsString();
                    Console.WriteLine($"Added new user. Name: {retrievedName}, E-mail: {retrievedEmail}");
                }
                transaction.Commit();
            }
        }
        return response;
    }
    // end::insert[]
    // tag::get[]
    List<string> GetFilesByUser(IDatabase database, string name, bool inference = false) {
        List<string> files = new List<string>();
        using (ITypeDBSession session = database.Session(SessionType.Data)) {
            using (ITypeDBTransaction transaction = session.Transaction(TransactionType.Read)) {
                string query = $"match $u isa user, has full-name '{name}'; get;";
                IEnumerable<IConceptMap> users = transaction.Query.Get(query);
                int userCount = users.Count();
                if (userCount > 1) {
                    Console.WriteLine("Error: Found more than one user with that name.");
                } else if (userCount == 1) {
                    string fetchQuery = $@"
                        match
                        $fn == '{name}';
                        $u isa user, has full-name $fn;
                        $p($u, $pa) isa permission;
                        $o isa object, has path $fp;
                        $pa($o, $va) isa access;
                        $va isa action, has name 'view_file';
                        get $fp; sort $fp asc;";
                    IEnumerable<IConceptMap> response = transaction.Query.Get(fetchQuery);
                    int resultCounter = 0;
                    foreach (IConceptMap cm in response) {
                        resultCounter++;
                        files.Add(cm.Get("fp").AsAttribute().Value.AsString());
                        Console.WriteLine($"File #{resultCounter}: {cm.Get("fp").AsAttribute().Value.AsString()}");
                    }
                    if (resultCounter == 0) {
                        Console.WriteLine("No files found. Try enabling inference.");
                    }
                } else {
                    Console.WriteLine("Error: No users found with that name.");
                }
            }
        }
        return files;
    }
    // end::get[]
    // tag::update[]
    int UpdateFilePath(IDatabase database, string oldPath, string newPath) {
        int count = 0;
        using (ITypeDBSession session = database.Session(SessionType.Data)) {
            using (ITypeDBTransaction transaction = session.Transaction(TransactionType.Write)) {
                string updateQuery = $@"
                    match
                    $f isa file, has path '{oldPath}';
                    delete
                    $f has path '{oldPath}';
                    insert
                    $f has path '{newPath}';";
                IEnumerable<IConceptMap> response = transaction.Query.Update(updateQuery);
                count = response.Count();
                if (count > 0) {
                    transaction.Commit();
                    Console.WriteLine($"Total number of paths updated: {count}.");
                } else {
                    Console.WriteLine("No matched paths: nothing to update.");
                }
            }
        }
        return count;
    }
    // end::update[]
    // tag::delete[]
    bool DeleteFile(IDatabase database, string path) {
        using (ITypeDBSession session = database.Session(SessionType.Data)) {
            using (ITypeDBTransaction transaction = session.Transaction(TransactionType.Write)) {
                string matchQuery = $"match $f isa file, has path '{path}'; get;";
                IEnumerable<IConceptMap> response = transaction.Query.Get(matchQuery);
                int count = response.Count();
                if (count == 1) {
                    transaction.Query.MatchDelete($"match $f isa file, has path '{path}'; delete $f isa file;").Resolve();
                    transaction.Commit();
                    Console.WriteLine("The file has been deleted.");
                    return true;
                } else if (count > 1) {
                    Console.WriteLine("Matched more than one file with the same path.");
                    Console.WriteLine("No files were deleted.");
                    return false;
                } else {
                    Console.WriteLine("No files matched in the database.");
                    Console.WriteLine("No files were deleted.");
                    return false;
                }
            }
        }
    }
    // end::delete[]
    // tag::queries[]
    void Queries(ITypeDBDriver driver, string dbName) {
        Console.WriteLine("\nRequest 1 of 6: Fetch all users as JSON objects with full names and emails");
        List<JObject> users = FetchAllUsers(driver.Databases.Get(dbName));

        string newName = "Jack Keeper";
        string newEmail = "jk@typedb.com";
        Console.WriteLine("\nRequest 2 of 6: Add a new user with the full-name " + newName + " and email " + newEmail);
        InsertNewUser(driver.Databases.Get(dbName), newName, newEmail);

        string name = "Kevin Morrison";
        Console.WriteLine("\nRequest 3 of 6: Find all files that the user " + name + " has access to view (no inference)");
        List<string> noFiles = GetFilesByUser(driver.Databases.Get(dbName), name);

        Console.WriteLine("\nRequest 4 of 6: Find all files that the user " + name + " has access to view (with inference)");
        List<string> files = GetFilesByUser(driver.Databases.Get(dbName), name, true);

        string oldPath = "lzfkn.java";
        string newPath = "lzfkn2.java";
        Console.WriteLine("\nRequest 5 of 6: Update the path of a file from " + oldPath + " to " + newPath);
        int updatedFiles = UpdateFilePath(driver.Databases.Get(dbName), oldPath, newPath);

        string filePath = "lzfkn2.java";
        Console.WriteLine("\nRequest 6 of 6: Delete the file with path " + filePath);
        bool deleted = DeleteFile(driver.Databases.Get(dbName), filePath);
    }
    // end::queries[]
    // tag::connection[]
    ITypeDBDriver ConnectToTypeDB(Edition typedbEdition, string addr, string username = CLOUD_USERNAME, string password = CLOUD_PASSWORD, bool encryption = true) {
        switch (typedbEdition) {
            case Edition.Core:
                return TypeDB.Driver.Drivers.CoreDriver(addr);
            case Edition.Cloud:
                return TypeDB.Driver.Drivers.CloudDriver(addr, new TypeDBCredential(username, password, encryption));
            default:
                throw new InvalidOperationException("Invalid TypeDB edition specified.");
        }
    }
    // end::connection[]
    // tag::main[]
    static void Main(string[] args) {
        ITypeDBDriver driver = ConnectToTypeDB(TYPEDB_EDITION, SERVER_ADDR);
        if (driver.IsOpen()) {
            if (DbSetup(driver, DB_NAME)) {
                Queries(driver, DB_NAME);
                Environment.Exit(0);
            } else {
                Console.Error.WriteLine("Failed to set up the database. Terminating...");
                Environment.Exit(1);
            }
        } else {
            Console.Error.WriteLine("Failed to connect to TypeDB server. Terminating...");
            Environment.Exit(1);
        }
    }
    // end::main[]
    // end::code[]
}
