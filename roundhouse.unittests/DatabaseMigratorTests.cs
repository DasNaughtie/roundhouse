using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace roundhouse.nunittests
{
    using System.Configuration;
    using System.Runtime.InteropServices;
    using System.Security.Cryptography;

    using FakeItEasy;

    using NUnit.Framework;

    using roundhouse.consoles;
    using roundhouse.cryptography;
    using roundhouse.databases;
    using roundhouse.infrastructure.app;
    using roundhouse.infrastructure.extensions;
    using roundhouse.migrators;
    using NUnit.Framework;

    public class TestableDefaultDatabaseMigrator : DefaultDatabaseMigrator
    {
        // mocks
        public Database mockDatabase                               = A.Fake<Database>();
        public CryptographicService mockCryptoService              = A.Fake<CryptographicService>();
        public ConfigurationPropertyHolder configurationPropHolder = new DefaultConfiguration();

        // check variables
        public StringBuilder checkInfoLog = new StringBuilder();

        public TestableDefaultDatabaseMigrator() : base(A.Dummy<Database>(), A.Dummy<CryptographicService>(), A.Dummy<ConfigurationPropertyHolder>())
        {
            database                         = mockDatabase;
            crypto_provider                  = mockCryptoService;
            configuration                    = configurationPropHolder;
            restoring_database               = configuration.Restore;
            restore_path                     = configuration.RestoreFromPath;
            custom_restore_options           = configuration.RestoreCustomOptions;
            output_path                      = configuration.OutputPath;
            error_on_one_time_script_changes = !configuration.WarnOnOneTimeScriptChanges;
            is_running_all_any_time_scripts  = configuration.RunAllAnyTimeScripts;
        }

        public void SetRestoringDatabase(bool value)
        {
            restoring_database = value;
        }

        public void SetRestorePath(string path)
        {
            restore_path = path;
        } 

        public void ClearCheckLog()
        {
            checkInfoLog.Clear();
        }

        public void SetConfiguration(ConfigurationPropertyHolder overrideConfiguration)
        {
            configuration = overrideConfiguration;
        }

        protected override void log_info_event_on_bound_logger(string message, params object[] args)
        {
            checkInfoLog.AppendFormat(message, args);
        }
    }

    [TestFixture]
    public class DatabaseMigratorTests
    {
        [Test]
        public void CreateOrRestoreDatabase_WithoutDryRun_CreatesTheDatabase()
        {
            var sut = new TestableDefaultDatabaseMigrator();
            var fakeDB = A.Fake<Database>();
            sut.database = fakeDB;
            sut.SetRestoringDatabase(false);
            A.CallTo(() => fakeDB.database_name).Returns("DbName");
            A.CallTo(() => fakeDB.server_name).Returns("ServerName");
            A.CallTo(() => fakeDB.create_database_if_it_doesnt_exist("some script")).Returns(true);

            var created = sut.create_or_restore_database("some script");
            StringAssert.Contains("Creating DbName database on ServerName server with custom script.", sut.checkInfoLog.ToString());
            Assert.AreEqual(true, created);

            sut.ClearCheckLog();
            created = sut.create_or_restore_database(null);
            StringAssert.Contains("Creating DbName database on ServerName server if it doesn't exist.", sut.checkInfoLog.ToString());
            Assert.AreEqual(false, created);
        }

        [Test]
        public void CreateOrRestoreDatabase_WithDryRun_DoesNotCreateTheDatabase()
        {
            var sut = new TestableDefaultDatabaseMigrator();
            var fakeDB = A.Fake<Database>();
            sut.database = fakeDB;
            sut.SetRestoringDatabase(false);
            sut.SetConfiguration(new DefaultConfiguration() { DryRun = true});
            A.CallTo(() => fakeDB.database_name).Returns("DbName");
            A.CallTo(() => fakeDB.server_name).Returns("ServerName");
            A.CallTo(() => fakeDB.create_database_if_it_doesnt_exist("some script")).Returns(true);

            var created = sut.create_or_restore_database("some script");
            StringAssert.Contains("-DryRun-Would have created DbName database on ServerName server with custom script.", sut.checkInfoLog.ToString());
            Assert.AreEqual(false, created);

            sut.ClearCheckLog();
            created = sut.create_or_restore_database(null);
            StringAssert.Contains("-DryRun-Would have created DbName database on ServerName server (if it didn't exist).", sut.checkInfoLog.ToString());
            Assert.AreEqual(false, created);
        }

        [Test]
        public void CreateOrRestoreDatabase_WithoutDryRunAndRestoring_RestoresDbDuringCreation()
        {
            var sut = new TestableDefaultDatabaseMigrator();
            var fakeDB = A.Fake<Database>();
            sut.database = fakeDB;
            sut.SetRestoringDatabase(true);
            sut.SetRestorePath("SomePath");
            A.CallTo(() => fakeDB.database_name).Returns("DbName");
            A.CallTo(() => fakeDB.server_name).Returns("ServerName");
            A.CallTo(() => fakeDB.create_database_if_it_doesnt_exist("some script")).Returns(true);

            var created = sut.create_or_restore_database("some script");
            StringAssert.Contains("Restoring DbName database on ServerName server from path SomePath", sut.checkInfoLog.ToString());
            Assert.AreEqual(false, created);
            A.CallTo(() => fakeDB.restore_database(A.Dummy<String>(), A.Dummy<String>())).WithAnyArguments().MustHaveHappened();
        }

        [Test]
        public void CreateOrRestoreDatabase_WithDryRunAndRestoring_DoesNotRestoreDbDuringCreation()
        {
            var sut = new TestableDefaultDatabaseMigrator();
            var fakeDB = A.Fake<Database>();
            sut.database = fakeDB;
            sut.SetRestoringDatabase(true);
            sut.SetConfiguration(new DefaultConfiguration() { DryRun = true});
            sut.SetRestorePath("SomePath");
            A.CallTo(() => fakeDB.database_name).Returns("DbName");
            A.CallTo(() => fakeDB.server_name).Returns("ServerName");
            A.CallTo(() => fakeDB.create_database_if_it_doesnt_exist("some script")).Returns(true);

            var created = sut.create_or_restore_database("some script");
            StringAssert.Contains("-DryRun-Would have created DbName database on ServerName server with custom script.", sut.checkInfoLog.ToString());
            Assert.AreEqual(false, created);
            StringAssert.Contains("-DryRun-Would have restored DbName database on ServerName server from path SomePath.", sut.checkInfoLog.ToString()); 
            A.CallTo(() => fakeDB.restore_database(A.Dummy<String>(), A.Dummy<String>())).WithAnyArguments().MustNotHaveHappened();
        }
    }
}
