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
    using roundhouse.infrastructure;
    using roundhouse.infrastructure.app;
    using roundhouse.infrastructure.persistence;

    // remember SqlServerDatabase -> AdoNetDatabase -> DefaultDatabase<IDbConnection> -> Database

    public class TestableDatabase : SqlServerDatabase
    {
        // set up my stuff

        // Check variables
        public StringBuilder CheckSqlRan = new StringBuilder();

        public override string run_sql(string sql_to_run, ConnectionType connection_type)
        {
            CheckSqlRan.Append(sql_to_run);
            return sql_to_run;
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

        [Test]
        public void RunDatabaseSpecificTasks_ForAnyDatabase_ReturnsSqlItRan()
        {
            var sut = MakeTestableSut();
            var retVal = sut.run_database_specific_tasks();
            StringAssert.Contains("CREATE SCHEMA [SomeSchema]", retVal);
        }

        [Test]
        public void DeleteDatabaseIfItExists_ForAnyDatabase_ReturnsSqlItRan()
        {
            var sut = MakeTestableSut();
            var retVal = sut.delete_database_if_it_exists();
            StringAssert.Contains("DROP DATABASE [SomeDb]", retVal);
        }

        [Test]
        public void GenerateInsertVersionScript_ForSqlServerDatabase_ReturnsSql()
        {
            var sut = MakeTestableSut();
            var retVal = sut.generate_insert_version_and_get_version_id_script("somePath", "someVersion");
            StringAssert.IsMatch("INSERT INTO.*RoundhousE.*Version", retVal);
        }

        [Test]
        public void GenerateInsertScriptsRunScript_ForSqlServerDatabase_ReturnsSql()
        {
            var sut = MakeTestableSut();
            var sqlScript = sut.generate_insert_scripts_run_script("someName", "someSql'f", "someHash", A.Dummy<bool>(), 1);
            StringAssert.IsMatch("INSERT INTO.*ScriptsRun.*someName.*someSql''f.*someHash", sqlScript);
        }

        [Test]
        public void GenerateSupportTablesScript_ForSqlServerDatabase_ReturnsSql()
        {
            var sut = MakeTestableSut();
            var sqlScript = sut.generate_support_tables_script();
            StringAssert.IsMatch(@"CREATE TABLE RoundhousE\.\[Version\]", sqlScript);
        }

        // test factories
        private TestableDatabase MakeTestableSut()
        {
            var sut = new TestableDatabase
            {
                roundhouse_schema_name = "SomeSchema",
                database_name          = "SomeDb",
                repository             = A.Fake<IRepository>()
            };
            return sut;
        }
    }
}
