using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace roundhouse.nunittests
{
    using System.Runtime.InteropServices;

    using FakeItEasy;

    using NUnit.Framework;
    using roundhouse.databases.sqlserver;
    using roundhouse.infrastructure.app;

    // remember SqlServerDatabase -> AdoNetDatabase -> DefaultDatabase<IDbConnection> -> Database

    public class TestableDatabase : SqlServerDatabase
    {
        // set up my stuff

        // Check variables
        public StringBuilder CheckSqlRan = new StringBuilder();

        public override void run_sql(string sql_to_run, ConnectionType connection_type)
        {
            CheckSqlRan.Append(sql_to_run);
        }
    }

    [TestFixture]
    public class DatabaseTests
    {

        [Test]
        public void CreateRoundhouseSchemaIfItDoenstExist_ForSqlServer_RunsSqlServerSpecificScript()
        {
            var sut = MakeTestableSut();
            sut.create_roundhouse_schema_if_it_doesnt_exist();
            StringAssert.Contains("CREATE SCHEMA [SomeSchema]", sut.CheckSqlRan.ToString());
        }

        [Test]
        public void RunDatabaseSpecificTasks_ForSqlServer_CreatesRoundhouseSchema()
        {
            var sut = MakeTestableSut();
            sut.run_database_specific_tasks();
            StringAssert.Contains("CREATE SCHEMA [SomeSchema]", sut.CheckSqlRan.ToString());
        }

        [Test]
        public void SetRecoveryMode_SimpleForSqlServer_SetsItAppropriately()
        {
            var sut = MakeTestableSut();
            sut.set_recovery_mode(true);
            StringAssert.Contains("ALTER DATABASE [SomeDb] SET RECOVERY SIMPLE", sut.CheckSqlRan.ToString());
        }

        [Test]
        public void RestoreDatabase_ForSqlServer_RestoresAppropriately()
        {
            var sut = MakeTestableSut();
            var path = "somePath";
            var customRestoreOptions = "someOptions";
            sut.restore_database(path, customRestoreOptions);
            StringAssert.Contains("ALTER DATABASE [SomeDb] SET SINGLE_USER WITH ROLLBACK IMMEDIATE", sut.CheckSqlRan.ToString());
        }

        [Test]
        public void DeleteDatabase_ForSqlServer_DeletesIt()
        {
            var sut = MakeTestableSut();
            sut.delete_database_if_it_exists();
            StringAssert.Contains("sys.databases WHERE [name] = 'SomeDb' AND source_database_id is NULL", sut.CheckSqlRan.ToString());
        }

        [Test]
        public void RunSql_ForAnyDatabase_ReturnsSqlItRan()
        {
            var sut = MakeTestableSut();
            var returnVal = sut.run_sql("some sql", A.Dummy<ConnectionType>());
            Assert.AreEqual("some sql", returnVal);
        }

        // test factories
        private TestableDatabase MakeTestableSut()
        {
            var sut = new TestableDatabase();
            sut.roundhouse_schema_name = "SomeSchema";
            sut.database_name = "SomeDb";
            return sut;
        }


    }




}
