
namespace roundhouse.nunittests
{
    using System;
    using System.Text;

    using FakeItEasy;

    using NUnit.Framework;

    using roundhouse.consoles;
    using roundhouse.cryptography;
    using roundhouse.databases;
    using roundhouse.infrastructure.app;
    using roundhouse.migrators;

    public class TestableDefaultDatabaseMigrator : DefaultDatabaseMigrator
    {
        // mocks
        public Database mockDatabase                               = A.Fake<Database>();
        public CryptographicService mockCryptoService              = A.Fake<CryptographicService>();
        public ConfigurationPropertyHolder configurationPropHolder = new DefaultConfiguration();

        // check variables
        public StringBuilder checkInfoLog    = new StringBuilder();
        public StringBuilder checkDebugLog   = new StringBuilder();
        public StringBuilder checkWarningLog = new StringBuilder();

        public TestableDefaultDatabaseMigrator() : base(A.Dummy<Database>(), A.Dummy<CryptographicService>(), A.Dummy<ConfigurationPropertyHolder>())
        {
            database                         = mockDatabase;
            crypto_provider                  = mockCryptoService;
            configuration                    = configurationPropHolder;
            restoring_database               = configuration.Restore;
            restore_path                     = configuration.RestoreFromPath;
            custom_restore_options           = configuration.RestoreCustomOptions;
            output_path                      = configuration.OutputPath;
            throw_error_on_one_time_script_changes = !configuration.WarnOnOneTimeScriptChanges;
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

        public void SetThrowErrorOnOneTimeScriptChanges(bool willThrow)
        {
            throw_error_on_one_time_script_changes = willThrow;
        }

        protected override void log_info_event_on_bound_logger(string message, params object[] args)
        {
            checkInfoLog.AppendFormat(message, args);
        }

        protected override void log_debug_event_on_bound_logger(string message, params object[] args)
        {
            checkDebugLog.AppendFormat(message, args);
        }

        protected override void log_warning_event_on_bound_logger(string message, params object[] args)
        {
            checkWarningLog.AppendFormat(message, args);
        }

        public void HandleOneTimeAlreadyRun(
            string sqlToRun,
            string scriptName,
            bool runOnce,
            string repoVersion,
            string repoPath)
        {
            handle_one_time_already_run(sqlToRun, scriptName, runOnce, repoVersion, repoPath);
        }

        public void RunAllTheSqlStatements(
            string sqlToRun,
            string scriptName,
            bool runOnce,
            long versionId,
            string repoVersion,
            string repoPath,
            ConnectionType connectionType)
        {
            run_all_the_sql_statements(sqlToRun, scriptName, runOnce, versionId, repoVersion, repoPath, connectionType);
        }
    }

    [TestFixture]
    public class DatabaseMigratorTests
    {
        [Test]
        public void CreateOrRestoreDatabase_WithoutDryRun_CreatesTheDatabase()
        {
            var sut = this.MakeTestableSut(false);

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
            var sut = this.MakeTestableSut(true);

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
            var sut = this.MakeTestableSut(false);
            sut.SetRestoringDatabase(true);

            var created = sut.create_or_restore_database("some script");
            StringAssert.Contains("Restoring DbName database on ServerName server from path SomePath", sut.checkInfoLog.ToString());
            Assert.AreEqual(false, created);
            A.CallTo(() => sut.database.restore_database(A.Dummy<String>(), A.Dummy<String>())).WithAnyArguments().MustHaveHappened();
        }

        [Test]
        public void CreateOrRestoreDatabase_WithDryRunAndRestoring_DoesNotRestoreDbDuringCreation()
        {
            var sut = this.MakeTestableSut(true);
            sut.SetRestoringDatabase(true);

            var created = sut.create_or_restore_database("some script");
            StringAssert.Contains("-DryRun-Would have created DbName database on ServerName server with custom script.", sut.checkInfoLog.ToString());
            Assert.AreEqual(false, created);
            StringAssert.Contains("-DryRun-Would have restored DbName database on ServerName server from path SomePath.", sut.checkInfoLog.ToString()); 
            A.CallTo(() => sut.database.restore_database(A.Dummy<String>(), A.Dummy<String>())).WithAnyArguments().MustNotHaveHappened();
        }

        [Test]
        public void BackupDatabaseIfItExists_WithoutDryRun_DoesIt()
        {
            var sut = this.MakeTestableSut(false);

            sut.backup_database_if_it_exists();
            A.CallTo(() => sut.database.backup_database(A.Dummy<String>())).WithAnyArguments().MustHaveHappened();
        }

        [Test]
        public void BackupDatabaseIfItExists_WithDryRun_DoesNotBackup()
        {
            var sut = MakeTestableSut(true);
            sut.backup_database_if_it_exists();

            StringAssert.Contains("-DryRun-Would have attempted a backup on DbName database on ServerName server.", sut.checkInfoLog.ToString());
            A.CallTo(() => sut.database.backup_database(A.Dummy<String>())).WithAnyArguments().MustNotHaveHappened();
        }

        [Test]
        [TestCase(true, "Setting recovery mode to 'Simple' for database DbName.")]
        [TestCase(false, "Setting recovery mode to 'Full' for database DbName.")]
        public void SetRecoveryMode_WithoutDryRun_SetsRecoveryModeProperly(bool simple, string expected)
        {
            var sut = MakeTestableSut(false);
            sut.set_recovery_mode(simple);
            StringAssert.Contains(expected, sut.checkInfoLog.ToString());
            A.CallTo(() => sut.database.set_recovery_mode(simple)).MustHaveHappened();
        }

        [Test]
        [TestCase(true, "-DryRun-Would have set recovery mode to 'Simple' for database DbName.")]
        [TestCase(false, "-DryRun-Would have set recovery mode to 'Full' for database DbName.")]
        public void SetRecoveryMode_WithDryRun_DoesntSetsRecoveryMode(bool simple, string expected)
        {
            var sut = MakeTestableSut(true);
            sut.set_recovery_mode(simple);
            StringAssert.Contains(expected, sut.checkInfoLog.ToString());
            A.CallTo(() => sut.database.set_recovery_mode(A.Dummy<bool>())).WithAnyArguments().MustNotHaveHappened();
        }

        [Test]
        public void RunRoundhouseSupportTasks_WithoutDryRun_RunsThemAndLogs()
        {
            var sut = MakeTestableSut(false);
            sut.run_roundhouse_support_tasks();
            StringAssert.Contains("Creating [VersionTable] table if it doesn't exist.", sut.checkInfoLog.ToString());
            StringAssert.Contains("Creating [ScriptsRunTable] table if it doesn't exist.", sut.checkInfoLog.ToString());
            StringAssert.Contains("Creating [ScriptsRunErrorsTable] table if it doesn't exist.", sut.checkInfoLog.ToString());
            A.CallTo(() => sut.database.create_or_update_roundhouse_tables()).MustHaveHappened();
        }

        [Test]
        public void RunRoundhouseSupportTasks_WithDryRun_DoesntRunThemAndLogs()
        {
            var sut = MakeTestableSut(true);
            sut.run_roundhouse_support_tasks();
            StringAssert.Contains(" -DryRun-Would create [VersionTable] table if it didn't exist.", sut.checkInfoLog.ToString());
            StringAssert.Contains(" -DryRun-Would create [ScriptsRunTable] table if it didn't exist.", sut.checkInfoLog.ToString());
            StringAssert.Contains(" -DryRun-Would create [ScriptsRunErrorsTable] table if it didn't exist.", sut.checkInfoLog.ToString());
            A.CallTo(() => sut.database.create_or_update_roundhouse_tables()).MustNotHaveHappened();
        }

        [Test]
        public void DeleteDatabase_WithoutDryRun_DeletesAndLogs()
        {
            var sut = MakeTestableSut(false);
            sut.delete_database();
            StringAssert.Contains("Deleting DbName database on ServerName server if it exists.", sut.checkInfoLog.ToString());
            A.CallTo(() => sut.database.delete_database_if_it_exists()).MustHaveHappened();
        }

        [Test]
        public void DeleteDatabase_WithDryRun_DoesntDeleteAndLogs()
        {
            var sut = MakeTestableSut(true);
            sut.delete_database();
            StringAssert.Contains("-DryRun-Would have deleted DbName database on ServerName server if it existed.", sut.checkInfoLog.ToString());
            A.CallTo(() => sut.database.delete_database_if_it_exists()).MustNotHaveHappened();
        }

        [Test]
        public void VersionTheDatabase_WithoutDryRun_VersionsAndLogs()
        {
            var sut = MakeTestableSut(false);
            sut.version_the_database("SomePath", "SomeVersion");
            StringAssert.Contains("Versioning DbName database with version SomeVersion based on SomePath", sut.checkInfoLog.ToString());
            A.CallTo(() => sut.database.insert_version_and_get_version_id("SomePath", "SomeVersion")).MustHaveHappened();
        }

        [Test]
        public void VersionTheDatabase_WithDryRun_DoesntVersionAndLogs()
        {
            var sut = MakeTestableSut(true);
            sut.version_the_database("SomePath", "SomeVersion");
            StringAssert.Contains("-DryRun-Would version DbName database with version SomeVersion based on SomePath", sut.checkInfoLog.ToString());
            A.CallTo(() => sut.database.insert_version_and_get_version_id(A.Dummy<String>(), A.Dummy<String>())).WithAnyArguments().MustNotHaveHappened();
        }

        [Test]
        public void RecordScript_WithoutDryRun_RecordsAndLogs()
        {
            var sut = MakeTestableSut(false);
            sut.record_script_in_scripts_run_table_is_dry_run_safe("SomeName", "SomeSQL", true, 0);
            StringAssert.Contains("Recording SomeName script ran on ServerName - DbName.", sut.checkDebugLog.ToString());
            A.CallTo(() => sut.database.insert_script_run(A.Dummy<String>(), A.Dummy<String>(), A.Dummy<String>(), A.Dummy<bool>(), A.Dummy<long>())).WithAnyArguments().MustHaveHappened();
        }


        [Test]
        public void RecordScript_WithDryRun_DoesNotRecordsAndLogs()
        {
            var sut = MakeTestableSut(true);
            sut.record_script_in_scripts_run_table_is_dry_run_safe("SomeName", "SomeSQL", true, 0);
            StringAssert.Contains("-DryRun-Would record SomeName script ran on ServerName - DbName in the ScriptsRunTable table.", sut.checkInfoLog.ToString());
            A.CallTo(() => sut.database.insert_script_run(A.Dummy<String>(), A.Dummy<String>(), A.Dummy<String>(), A.Dummy<bool>(), A.Dummy<long>())).WithAnyArguments().MustNotHaveHappened();
        }

        [Test]
        public void RecordScriptInErrorsRun_WithoutDryRun_RecordsAndLogs()
        {
            var sut = MakeTestableSut(false);
            sut.record_script_in_scripts_run_errors_table_is_dry_run_safe("SomeName", "SomeSQL", "SomeSQLError", "SomeMessage", "SomeVersion", "SomePath");
            StringAssert.Contains("Recording SomeName script ran with error on ServerName - DbName.", sut.checkDebugLog.ToString());
            A.CallTo(() => sut.database.insert_script_run_error(A.Dummy<String>(), A.Dummy<String>(), 
                A.Dummy<String>(), A.Dummy<String>(), A.Dummy<String>(), 
                A.Dummy<String>())).WithAnyArguments().MustHaveHappened();
        }        [Test]        public void RecordScriptInErrorsRun_WithDryRun_DoesNotRecordAndLogs()        {            var sut = MakeTestableSut(true);            sut.record_script_in_scripts_run_errors_table_is_dry_run_safe("SomeName", "SomeSQL", "SomeSQLError", "SomeMessage", "SomeVersion", "SomePath");            StringAssert.Contains("-DryRun-Would have recorded SomeName script ran with error on ServerName - DbName in the ScriptsRunErrorsTable table.",                 sut.checkInfoLog.ToString());            A.CallTo(() => sut.database.insert_script_run_error(A.Dummy<String>(), A.Dummy<String>(),                 A.Dummy<String>(), A.Dummy<String>(), A.Dummy<String>(),                 A.Dummy<String>())).WithAnyArguments().MustNotHaveHappened();        }

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void HandleOneTimeAlreadyRun_WithChangedAlreadyRunOneTimeScript_Throws(bool dryRun)
        {
            var sut = MakeTestableSut(dryRun);
            A.CallTo(() => sut.database.get_current_script_hash("SomeName")).Returns("SomeHash");
            A.CallTo(() => sut.mockCryptoService.hash(A.Dummy<String>())).WithAnyArguments().Returns("SomeOtherHash");
            A.CallTo(() => sut.database.has_run_script_already("SomeName")).Returns(true);
            var ex =
                Assert.Catch(
                    () => sut.HandleOneTimeAlreadyRun("SomeSQL", "SomeName", true, "SomeRepoVersion", "SomePath"));
            StringAssert.Contains("SomeName has changed since the last time it was run. By default this is not allowed", ex.Message);

            sut.SetThrowErrorOnOneTimeScriptChanges(false);
            sut.HandleOneTimeAlreadyRun("SomeSQL", "SomeName", true, "SomeRepoVersion", "SomePath");
            StringAssert.Contains("SomeName is a one time script that has changed since it was run.", sut.checkWarningLog.ToString());
        }        [Test]        [TestCase(false)]        [TestCase(true)]        public void HandleOneTimeAlreadyRun_WithUnchangedAlreadyRunOneTimeScript_Ignores(bool dryRun)        {            var sut = MakeTestableSut(dryRun);            A.CallTo(() => sut.database.get_current_script_hash("SomeName")).Returns("SomeHash");            A.CallTo(() => sut.mockCryptoService.hash(A.Dummy<String>())).WithAnyArguments().Returns("SomeHash");            A.CallTo(() => sut.database.has_run_script_already("SomeName")).Returns(true);
            sut.HandleOneTimeAlreadyRun("SomeSQL", "SomeName", true, "SomeRepoVersion", "SomePath");
            Assert.IsNullOrEmpty(sut.checkInfoLog.ToString());        }

        [Test]
        public void RunAllTheSqlStatements_WithoutDryRun_RunsAndLogs()
        {
            var sut = MakeTestableSut(false);
            sut.RunAllTheSqlStatements("SomeSQL", "SomeName", true, 1, "SomeVersion", "SomePath", ConnectionType.Default);
            StringAssert.Contains(" Running SomeName on ServerName - DbName.", sut.checkInfoLog.ToString());
            A.CallTo(() => sut.database.run_sql("SomeSQL", ConnectionType.Default)).MustHaveHappened();
        }

        [Test]
        public void RunAllTheSqlStatements_WithDryRun_DoesNotRunAndLogs()
        {
            var sut = MakeTestableSut(true);
            sut.RunAllTheSqlStatements("SomeSQL", "SomeName", true, 1, "SomeVersion", "SomePath", ConnectionType.Default);
            StringAssert.Contains(" -DryRun-Would have run SomeName on ServerName - DbName.", sut.checkInfoLog.ToString());
            StringAssert.Contains("-DryRun-Would record SomeName script ran on ServerName - DbName", sut.checkInfoLog.ToString());
            A.CallTo(() => sut.database.run_sql("SomeSQL", ConnectionType.Default)).WithAnyArguments().MustNotHaveHappened();
        }

        [Test]
        [TestCase(false, true, false, false, true, "", "")] // runEvery, notThere, noChange
        [TestCase(false, true, true, false, true, "", "")] // runEvery, alreadyThere, noChange
        [TestCase(false, true, true, true, true, "", "")] // runEvery, alreadyThere, changed
        [TestCase(false, true, true, true, true, "", "")] // runEvery, notThere, changed // TODO (PMO): Should this throw exception? notThere and changed?

        [TestCase(true, false, false, false, true, "", "")] // runOnce, notThere, noChange
        [TestCase(true, false, true, false, false, " Skipped SomeName - One time script.", "")] // runOnce, alreadyThere, noChange
        //[TestCase(true, false, true, true, false, " Skipped SomeName - One time script.", "")] // runOnce, already there, changed // should throw exception
        [TestCase(true, false, false, true, true, "", "")] // runOnce, notThere, changed // TODO (PMO):  Should this throw exception? notThere and changed?
        //[TestCase(false, false, false, false, false, "", "")] // runNeither, not there, noChange // TODO (PMO): Make this situation throw exception
        public void RunSql_WithoutDryRunAndRunnableScript_RunsAndLogs(bool runOnce, bool runEveryTime, bool alreadyRun, bool scriptChanged, bool expectedToRun, string expectedInfo, string expectedWarning)
        {
            var sut = MakeTestableSut(false);

            if (alreadyRun)
            {
                A.CallTo(() => sut.database.has_run_script_already("SomeName")).Returns(true);
            }
            else
            {
                A.CallTo(() => sut.database.has_run_script_already("SomeName")).Returns(false);
            }

            if (scriptChanged)
            {
                A.CallTo(() => sut.database.get_current_script_hash("SomeName")).Returns("SomeHash");
                A.CallTo(() => sut.mockCryptoService.hash(A.Dummy<String>())).WithAnyArguments().Returns("DifferentHash");
            }
            else
            {
                A.CallTo(() => sut.database.get_current_script_hash("SomeName")).Returns("SomeHash");
                A.CallTo(() => sut.mockCryptoService.hash(A.Dummy<String>())).WithAnyArguments().Returns("SomeHash");
            }

            var sqlRan = sut.run_sql(
                "SomeSql",
                "SomeName",
                runOnce,
                runEveryTime,
                1,
                A.Dummy<environments.Environment>(),
                "SomeVersion",
                "SomePath",
                ConnectionType.Default);
            Assert.AreEqual(expectedToRun, sqlRan);
            StringAssert.Contains(expectedInfo, sut.checkInfoLog.ToString());
            StringAssert.Contains(expectedWarning, sut.checkWarningLog.ToString());        }        // ----
        private TestableDefaultDatabaseMigrator MakeTestableSut(bool dryRun)
        {
            var sut = new TestableDefaultDatabaseMigrator();
            var fakeDB = A.Fake<Database>();
            sut.database = fakeDB;
            sut.SetConfiguration(new DefaultConfiguration() { DryRun = dryRun});
            sut.SetRestorePath("SomePath");
            A.CallTo(() => fakeDB.database_name).Returns("DbName");
            A.CallTo(() => fakeDB.server_name).Returns("ServerName");
            A.CallTo(() => fakeDB.version_table_name).Returns("VersionTable");
            A.CallTo(() => fakeDB.scripts_run_table_name).Returns("ScriptsRunTable");
            A.CallTo(() => fakeDB.scripts_run_errors_table_name).Returns("ScriptsRunErrorsTable");
            A.CallTo(() => fakeDB.create_database_if_it_doesnt_exist("some script")).Returns(true);

            return sut;
        }

    }
}
