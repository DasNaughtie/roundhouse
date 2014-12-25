using roundhouse.databases;
using roundhouse.databases.access;

namespace roundhouse.unittests
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.IO;

    using FakeItEasy;
    using NUnit.Framework;
    using roundhouse.consoles;
    using roundhouse.databases.sqlserver;
    using roundhouse.environments;
    using roundhouse.folders;
    using roundhouse.infrastructure.app;
    using roundhouse.infrastructure.extensions;
    using roundhouse.infrastructure.filesystem;
    using roundhouse.infrastructure.logging;
    using roundhouse.migrators;
    using roundhouse.resolvers;
    using roundhouse.runners;
    using System.Text;

    using Environment = roundhouse.environments.Environment;

    public class TestableRoundhouseMigrationRunner : RoundhouseMigrationRunner
    {
        public Logger mockLogger                                   = A.Fake<Logger>();
        public DatabaseMigrator fakeDbMigrator                     = A.Fake<DatabaseMigrator>();
        public DefaultConfiguration fakeConfiguration              = A.Fake<DefaultConfiguration>();
        public DefaultEnvironment fakeEnvironment                  = A.Fake<DefaultEnvironment>();
        public KnownFolders fakeKnownFolders                       = A.Fake<KnownFolders>();
        public WindowsFileSystemAccess fakeFileSystemAccess        = A.Fake<WindowsFileSystemAccess>();
        public VersionResolver fakeVersionResolver                 = A.Fake<VersionResolver>();

        // Quick stub checks
        public bool CheckChangeDropFolderCreated       = false;
        public StringBuilder CheckLogWritten           = new StringBuilder();
        public StringBuilder CheckDebugLog             = new StringBuilder();
        public StringBuilder CheckWarningWritten       = new StringBuilder();
        public StringBuilder CheckFilesCopied          = new StringBuilder();
        public string CheckCombinedScriptFileCreatedAt = null;

        public TestableRoundhouseMigrationRunner()
            : base(A.Dummy<String>(),
                A.Dummy<Environment>(),
                A.Dummy<KnownFolders>(),
                A.Dummy<FileSystemAccess>(),
                A.Dummy<DatabaseMigrator>(), 
                A.Dummy<VersionResolver>(),
                false,
                false,
                false,
                false,
                true,
                A.Dummy<ConfigurationPropertyHolder>())
        {
            repository_path   = "repo_path";
            environment       = fakeEnvironment;
            known_folders     = fakeKnownFolders;
            file_system       = fakeFileSystemAccess;
            database_migrator = fakeDbMigrator;
            version_resolver  = fakeVersionResolver;
            configuration     = fakeConfiguration;

        }

        protected override Logger get_bound_logger()
        {
            return mockLogger;
        }

        protected override void WaitForKeypress()
        {
            return;
        }

        protected override void create_change_drop_folder()
        {
            CheckChangeDropFolderCreated = true;
        }

        protected override void log_debug_event_on_bound_logger(string message, params object[] args)
        {
            CheckDebugLog.AppendFormat(message, args);
        }

        protected override void log_info_event_on_bound_logger(string message, params object[] args)
        {
            CheckLogWritten.AppendFormat(message, args);
        }

        protected override void log_warning_event_on_bound_logger(string message, params object[] args)
        {
            CheckWarningWritten.AppendFormat(message, args);
        }

        protected override bool does_directory_exist(string directory)
        {
            return true;
        }

        protected override string[] get_the_names_of_all_files_in_directory_nonrecursively(string directory)
        {
            return new string[] {"file1.sql", "file2.sql", "file3.sql"};
        }

        public override string get_file_text(string file_location)
        {
            return "SELECT * FROM TABLE;";
        }

        protected override string[] get_the_names_of_files_in_directory_recursively(string directory)
        {
            return new string[] {"file1.sql", "file2.sql", "file3.sql", "file4.sql", "file5.sql"};
        }

        protected override string[] get_the_names_of_directories_in_directory(string directory)
        {
            // to avoid infinite recursion
            if (directory == RoundhouseMigrationRunnerTests.FOLDER_PATH)
            {
                return new string[] {"directory1", "directory2"};
            }
            else
            {
                return new string[] {};
            }
        }

        protected override void file_copy_unsafe(string sql_file_ran, string destination_file)
        {
            CheckFilesCopied.AppendFormat("{0} -> {1}{2}", sql_file_ran, destination_file, System.Environment.NewLine);
        }

        protected override void create_default_combined_script_path_file_if_needed(string combinedScriptFilePath)
        {
            CheckCombinedScriptFileCreatedAt = combinedScriptFilePath;
            base.create_default_combined_script_path_file_if_needed(combinedScriptFilePath);
        }

        protected override string[] get_lines_from_file(string path)
        {
            if (path.EndsWith(COMBINED_SCRIPT_FILE_NAME))
            {
                return new string[] { LINE_MARKER_DURING_MIGRATION, "Line from " + path, LINE_MARKER_AFTER_MIGRATION };
            }
            else
            {
                return new string[] { "Line from " + path };
            }
        }

        public void CopyToChangeDropFolder(string sql_file_ran, Folder migration_folder)
        {
            base.copy_to_change_drop_folder(sql_file_ran, null, migration_folder, ExecutionPhase.During);
        }

        public void CopyToChangeDropFolder(string sql_file_ran, string new_script_name, Folder migration_folder, ExecutionPhase phase)
        {
           base.copy_to_change_drop_folder(sql_file_ran, new_script_name, migration_folder, phase); 
        }
    }

    [TestFixture]
    public class RoundhouseMigrationRunnerTests
    {
        public const string REPO_PATH         = "repo_path";
        public const string FOLDER_PATH       = "folderpath";
        public readonly string NEW_DB_VERSION = "2";

        [Test]
        public void Run_WithNormalConfiguration_LogsThatWeAreAboutToKick()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false, false);
            sut.run();
            StringAssert.Contains("Please press enter when ready to kick", sut.CheckLogWritten.ToString());
            Assert.AreEqual(true, sut.CheckChangeDropFolderCreated);
        }

        [Test]
        [TestCase(true, "This is a dry run", true)]
        [TestCase(false, "This is a dry run", false)]
        public void Run_WithOrWithoutDryRun_LogsAppropriateInfoActions(bool isDryRun, string testString, bool shouldContain)
        {
            var sut = MakeTestableRoundhouseMigrationRunner(isDryRun, false);
            sut.run();
            if (shouldContain)
            {
                StringAssert.Contains(testString, sut.CheckLogWritten.ToString());
            }
            else
            {
                StringAssert.DoesNotContain(testString, sut.CheckLogWritten.ToString());
            }
        }

        [Test]
        public void Run_WithoutDryRun_WillDropDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false, false);
            sut.dropping_the_database = true;
            sut.run();
            A.CallTo(() => sut.database_migrator.open_admin_connection()).MustHaveHappened();
            A.CallTo(() => sut.database_migrator.delete_database()).WithAnyArguments().MustHaveHappened();
            A.CallTo(() => sut.database_migrator.close_admin_connection()).MustHaveHappened();
            A.CallTo(() => sut.database_migrator.close_connection()).MustHaveHappened();
            StringAssert.Contains("has removed database", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithDryRun_WillNotDropDatabaseButWillCallMethodAndLog()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true, false);
            sut.dropping_the_database = true;
            sut.run();
            StringAssert.Contains("would have removed database (DbName)", sut.CheckLogWritten.ToString());
            A.CallTo(() => sut.database_migrator.delete_database()).WithAnyArguments().MustHaveHappened();
            A.CallTo(() => sut.database_migrator.close_connection()).MustHaveHappened();
        }

        [Test]
        public void Run_WithDryRun_WillNotCreateTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true, false);
            sut.dont_create_the_database = false;
            sut.run();
            A.CallTo(() => sut.database_migrator.create_or_restore_database(A.Dummy<String>())).WithAnyArguments().MustHaveHappened();
        }

        [Test]
        public void DropTheDatabase_WithoutDryRun_CopiesDropScriptToChangeDropFolder()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false, true);
            sut.dropping_the_database = true;
            sut.run();
            StringAssert.IsMatch(@"1\d\d_DropDatabase.sql", sut.CheckFilesCopied.ToString());
        }

        [Test]
        public void DropTheDatabase_WithDryRun_CopiesDropScriptToChangeDropFolder()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true, true);
            sut.dropping_the_database = true;
            sut.run();
            StringAssert.IsMatch(@"1\d\d_DropDatabase.sql", sut.CheckFilesCopied.ToString());
        }

        [Test]
        public void Run_WithoutDryRun_CreatesCreateDatabaseScript()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false, true);
            sut.dont_create_the_database = false;
            A.CallTo(() => sut.database_migrator.create_or_restore_database(A.Dummy<String>())).WithAnyArguments().Returns(true);
            sut.run();
            StringAssert.Contains("_CreateOrRestoreDatabase.sql", sut.CheckFilesCopied.ToString());
        }

        [Test]
        [TestCase(RecoveryMode.Simple, @"1\d\d_SetRecoveryMode.sql")]
        [TestCase(RecoveryMode.Full, @"1\d\d_SetRecoveryMode.sql")]
        public void Run_WithSimpleOrFullModeRecovery_CreatesScriptForIt(RecoveryMode recoveryMode, string expectedFileName)
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false, true);
            sut.fakeConfiguration.RecoveryMode = recoveryMode;
            sut.run();
            StringAssert.IsMatch(expectedFileName, sut.CheckFilesCopied.ToString());
        }

        [Test]
        public void Run_WithNoChangeRecovery_DoesNotCreateScriptForIt()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false, true);
            sut.fakeConfiguration.RecoveryMode = RecoveryMode.NoChange;
            sut.run();
            StringAssert.DoesNotContain("_SetRecoveryMode.sql", sut.CheckFilesCopied.ToString());
        }

        [Test]
        public void Run_WithoutDryRun_WillCreateTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false, false);
            sut.dont_create_the_database = false;
            sut.run();
            A.CallTo(() => sut.database_migrator.create_or_restore_database(A.Dummy<String>())).WithAnyArguments().MustHaveHappened();
        }

        [Test]
        [TestCase(RecoveryMode.Full)]
        [TestCase(RecoveryMode.Simple)]
        public void Run_WithoutDryRun_WillSetRecoveryMode(RecoveryMode recoveryMode)
        {

            var sut = MakeTestableRoundhouseMigrationRunner(false, false);
            sut.fakeConfiguration.RecoveryMode = recoveryMode;
            sut.run();
            A.CallTo(() => sut.database_migrator.open_connection(true)).WithAnyArguments().MustHaveHappened();
            A.CallTo(() => sut.database_migrator.set_recovery_mode(recoveryMode == RecoveryMode.Simple)).MustHaveHappened();
        }

        [Test]
        [TestCase(RecoveryMode.Full)]
        [TestCase(RecoveryMode.Simple)]
        public void Run_WithDryRun_WillCallRecoveryMode(RecoveryMode recoveryMode, string expected)
        {

            var sut = MakeTestableRoundhouseMigrationRunner(true, false);
            sut.fakeConfiguration.RecoveryMode = recoveryMode;
            sut.run();
            StringAssert.Contains(expected, sut.CheckLogWritten.ToString());
            A.CallTo(() => sut.database_migrator.set_recovery_mode(recoveryMode == RecoveryMode.Simple)).MustHaveHappened();
        }

        [Test]
        public void Run_WithSimpleOutput_ClearsOutCombinedScriptFile()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(A.Dummy<bool>(), true);
            sut.run();
            StringAssert.Contains("combinedScripts.sql", sut.CheckCombinedScriptFileCreatedAt);
        }

        //[Test] // this is an integration test, taking out for now...
        public void CopyToChangeDropFolder_WithSimpleOutput_CreatesAllSqlFileInAdditionToIndividualScripts()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true, true);
            var temp1 = Path.GetTempFileName();
            var temp2 = Path.GetTempFileName();
            var temp3 = Path.GetTempFileName();
            File.WriteAllText(temp1, "Text for temp1 -- after");
            var folder1 = MakeMigrationsFolder("After", false, true);

            File.WriteAllText(temp2, "Text for temp2 -- during");
            var folder2 = MakeMigrationsFolder("During", false, false);

            File.WriteAllText(temp3, "Text for temp3 -- before");
            var folder3 = MakeMigrationsFolder("Before", true, false);

            sut.CopyToChangeDropFolder(temp1, "Temp1After", folder1, RoundhouseMigrationRunner.ExecutionPhase.After);
            sut.CopyToChangeDropFolder(temp2, "Temp2During", folder2, RoundhouseMigrationRunner.ExecutionPhase.During);
            sut.CopyToChangeDropFolder(temp3, "Temp3During", folder3, RoundhouseMigrationRunner.ExecutionPhase.Before);

            var combinedScriptPath = sut.get_combined_script_file_path();

            var contents = File.ReadAllLines(combinedScriptPath);

            StringAssert.Contains("Text for temp3 -- before", contents[0]);
            StringAssert.Contains("Text for temp2 -- during", contents[2]);
            StringAssert.Contains("Text for temp1 -- after", contents[4]);
        }

        [Test]
        [TestCase(true, true, "Would have began a transaction on database DbName", null)]
        [TestCase(true, false, null, "Would have began a transaction on database DbName")]
        [TestCase(false, true, null, "Would have began a transaction on database DbName")]
        [TestCase(false, false, null, "Would have began a transaction on database DbName")]
        public void Run_WithDryRunAndTransactionRequestedAndDbThatSupportsTransactions_WillNotBeginTransactionInDatabase(
            bool runInTransaction, 
            bool supportsTransaction, 
            string expected, 
            string notExpected)
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true, false);
            sut.run_in_a_transaction = runInTransaction;
            if (supportsTransaction)
            {
                var fakeDb = A.Fake<SqlServerDatabase>();
                sut.database_migrator.database = fakeDb; //new SqlServerDatabase();
                A.CallTo(() => fakeDb.has_roundhouse_support_tables()).WithAnyArguments().Returns(true);
                A.CallTo(() => fakeDb.supports_ddl_transactions).Returns(true);
            }
            else
            {
                var fakeDb = A.Fake<SqlServerDatabase>();
                sut.database_migrator.database = fakeDb; // new AccessDatabase();
                A.CallTo(() => fakeDb.has_roundhouse_support_tables()).WithAnyArguments().Returns(true); 
            }
            sut.database_migrator.database.database_name = "DbName";
            sut.run();
            if (string.IsNullOrEmpty(expected) == false)
            {
                StringAssert.Contains(expected, sut.CheckLogWritten.ToString());
            }
            else
            {
                StringAssert.DoesNotContain(notExpected, sut.CheckLogWritten.ToString());
            }
        }

        [Test]
        [TestCase(true, true, "Would have began a transaction on database DbName")]
        [TestCase(true, false, "Would have began a transaction on database DbName")]
        [TestCase(false, true, "Would have began a transaction on database DbName")]
        [TestCase(false, false, "Would have began a transaction on database DbName")]
        public void Run_WithoutDryRunAndTransactionRequestedAndDbThatSupportsTransactions_WillBeginTransaction(
            bool runInTransaction,
            bool supportsTransaction,
            string notExpected)
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false, false);
            sut.run_in_a_transaction = runInTransaction;
            if (supportsTransaction)
            {
                var fakeDb = A.Fake<SqlServerDatabase>();
                sut.database_migrator.database = fakeDb; //new SqlServerDatabase();
                A.CallTo(() => fakeDb.has_roundhouse_support_tables()).WithAnyArguments().Returns(true);
                A.CallTo(() => fakeDb.supports_ddl_transactions).Returns(true);
            }
            else
            {
                var fakeDb = A.Fake<SqlServerDatabase>();
                sut.database_migrator.database = fakeDb; // new AccessDatabase();
                A.CallTo(() => fakeDb.has_roundhouse_support_tables()).WithAnyArguments().Returns(true);
            }
            sut.run();
            StringAssert.DoesNotContain(notExpected, sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithDryRunAndSimpleOutput_DoesRunRoundhouseSupportTasksAndCreatesScriptFile()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true, true);
            sut.run();
            StringAssert.Contains("Would have run roundhouse support tasks on database DbName", sut.CheckLogWritten.ToString());
            A.CallTo(() => sut.database_migrator.run_roundhouse_support_tasks()).MustHaveHappened();
            StringAssert.IsMatch(@"1\d\d_DatabaseSpecificTasks.sql", sut.CheckFilesCopied.ToString());
        }

        [Test]
        public void Run_WithoutDryRunAndSimpleOutput_RunsSupportTasksAndCreatesScriptFile()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false, true);
            sut.run();
            StringAssert.DoesNotContain("Would have run roundhouse support tasks on database DbName", sut.CheckLogWritten.ToString());
            A.CallTo(() => sut.database_migrator.run_roundhouse_support_tasks()).MustHaveHappened();
            StringAssert.IsMatch(@"1\d\d_DatabaseSpecificTasks.sql", sut.CheckFilesCopied.ToString());
        }

        [Test]
        public void Run_WithDryRun_DoesCallVersionTheDatabaseBecauseItIsDryRunSafe()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true, false);
            sut.run();
            StringAssert.Contains("Would have migrated database DbName from version 1 to 2", sut.CheckLogWritten.ToString());
            A.CallTo(() => sut.database_migrator.get_current_version(REPO_PATH)).MustHaveHappened();
            A.CallTo(() => sut.database_migrator.version_the_database(REPO_PATH, NEW_DB_VERSION)).MustHaveHappened();
        }

        [Test]
        public void Run_WithoutDryRun_DoesInsertNewVersionRowIntoTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false, false);
            sut.run();
            StringAssert.Contains("Migrating DbName from version 1 to 2", sut.CheckLogWritten.ToString());
            A.CallTo(() => sut.database_migrator.get_current_version(REPO_PATH)).MustHaveHappened();
            A.CallTo(() => sut.database_migrator.version_the_database(REPO_PATH, NEW_DB_VERSION)).MustHaveHappened();
        }

        [Test]
        public void Run_WithDryRun_DoesLogAndTraverseFoldersBecauseRunSqlIsDryRunSafe()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true, false);
            A.CallTo(() => sut.database_migrator.run_sql("", "", true, true, 0, A.Dummy<Environment>(), "", "", A.Dummy<ConnectionType>())).WithAnyArguments().Returns(true);
            sut.run();
            StringAssert.Contains("Looking for friendly-alter scripts in \"" + FOLDER_PATH + "\\alter\" (every time scripts)", sut.CheckLogWritten.ToString());
            StringAssert.Contains("Looking for friendly-functions scripts in \"" + FOLDER_PATH + "\\functions\" (one-time only scripts)", sut.CheckLogWritten.ToString());
            StringAssert.Contains("file1.sql ->", sut.CheckFilesCopied.ToString());
            A.CallTo(() => sut.database_migrator.run_sql("", "", true, true, 0, A.Dummy<Environment>(), "", "", A.Dummy<ConnectionType>())).WithAnyArguments().MustHaveHappened();
        }

        [Test]
        public void Run_WithoutDryRun_DoesLogAndTraverseFolders()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false, false);
            A.CallTo(() => sut.database_migrator.run_sql("", "", true, true, 0, A.Dummy<Environment>(), "", "", A.Dummy<ConnectionType>())).WithAnyArguments().Returns(true);
            sut.run();
            StringAssert.Contains("Looking for friendly-alter scripts in \"" + FOLDER_PATH + "\\alter\" (every time scripts)", sut.CheckLogWritten.ToString());
            StringAssert.Contains("Looking for friendly-functions scripts in \"" + FOLDER_PATH + "\\functions\" (one-time only scripts)", sut.CheckLogWritten.ToString());
            StringAssert.Contains("file1.sql ->", sut.CheckFilesCopied.ToString());
            A.CallTo(() => sut.database_migrator.run_sql("", "", true, true, 0, A.Dummy<Environment>(), "", "", A.Dummy<ConnectionType>())).WithAnyArguments().MustHaveHappened();
        }

        [Test]
        public void Run_WithoutDryRun_TellsYouYourDatabaseWasKicked()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false, false);
            sut.run();
            StringAssert.IsMatch("RoundhousE v([0-9.]*) has kicked your database \\(DbName\\)\\! You are now at version " + NEW_DB_VERSION + 
                "\\. All changes and backups can be found at \"" + FOLDER_PATH + "\\\\change_drop\"", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithDryRun_TellsYouYourDatabaseWouldHaveBeenKicked()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true, false);
            sut.run();
            StringAssert.IsMatch("-DryRun-RoundhousE v([0-9.]*) would have kicked your database \\(DbName\\)\\! You would be at version " + NEW_DB_VERSION
                + "\\. All changes and backups can be found at \"" + FOLDER_PATH + "\\\\change_drop\"", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithoutDryRunAndDatabaseDropRequested_DropsTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false, false);
            sut.dropping_the_database = true;
            sut.run();
            StringAssert.Contains("RoundhousE has removed database (DbName). All changes and backups can be found at \"" + FOLDER_PATH + "\\change_drop\"", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithDryRunAndDatabaseDropRequested_DoesntDropTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true, false);
            sut.dropping_the_database = true;
            sut.run();
            StringAssert.Contains("-DryRun-RoundhousE would have removed database (DbName). All changes and backups would be found at \"" + FOLDER_PATH + "\\change_drop\"", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithDryRunAgainstNonRoundhousEDatabase_GivesUsefulErrorMessageAndAScriptToRun()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true, true);
            A.CallTo(() => sut.database_migrator.database.has_roundhouse_support_tables()).WithAnyArguments().Returns(false);
            sut.run();
            StringAssert.Contains("Database DbName on server ServerName does not have the RoundhousE support", sut.CheckWarningWritten.ToString());
            StringAssert.Contains("tables (ScriptsRun, ScriptsRunErrors, Version) created.", sut.CheckWarningWritten.ToString());
        }

        [Test]
        public void CopyToChangeDropFolder_WithoutSimpleOutput_PutsOutputInMigrationFoldersWithoutNumbers()
        {
            var sut        = MakeTestableRoundhouseMigrationRunner(false, false);
            var dropFolder = MakeMigrationsFolder("folderName", A.Dummy<bool>(), A.Dummy<bool>());
            sut.CopyToChangeDropFolder("folder1\\file1.sql", dropFolder);
            sut.CopyToChangeDropFolder("folder2\\file2.sql", dropFolder);
            sut.CopyToChangeDropFolder("folder3\\file3.sql", dropFolder);
            StringAssert.Contains("-> " + FOLDER_PATH + "\\change_drop\\itemsRan\\folderName\\folder1\\file1.sql", sut.CheckFilesCopied.ToString());
            StringAssert.Contains("-> " + FOLDER_PATH + "\\change_drop\\itemsRan\\folderName\\folder2\\file2.sql", sut.CheckFilesCopied.ToString());
            StringAssert.Contains("-> " + FOLDER_PATH + "\\change_drop\\itemsRan\\folderName\\folder3\\file3.sql", sut.CheckFilesCopied.ToString());
        }

        [Test, Ignore]
        public void CopyToChangeDropFolder_WithSimpleOutput_PutsOutputInChangeDropFolderWithNumbers()
        {
            var sut        = MakeTestableRoundhouseMigrationRunner(false, true);
            var dropFolder = MakeMigrationsFolder("folderName", A.Dummy<bool>(), A.Dummy<bool>());
            sut.CopyToChangeDropFolder("folder1\\file1.sql", dropFolder);
            sut.CopyToChangeDropFolder("folder2\\file2.sql", dropFolder);
            sut.CopyToChangeDropFolder("folder3\\file3.sql", dropFolder);
            StringAssert.IsMatch("-> " + FOLDER_PATH + @"\\change_drop\\scripts\\individualScripts\\2\d\d_file1.sql", sut.CheckFilesCopied.ToString());
            StringAssert.IsMatch("-> " + FOLDER_PATH + @"\\change_drop\\scripts\\individualScripts\\2\d\d_file2.sql", sut.CheckFilesCopied.ToString());
            StringAssert.IsMatch("-> " + FOLDER_PATH + @"\\change_drop\\scripts\\individualScripts\\2\d\d_file3.sql", sut.CheckFilesCopied.ToString());
        }
        
        private TestableRoundhouseMigrationRunner MakeTestableRoundhouseMigrationRunner(bool dryRun, bool simpleOutput)
        {
            var sut                                      = new TestableRoundhouseMigrationRunner();
            sut.fakeConfiguration.DryRun                 = dryRun;
            sut.fakeConfiguration.SimpleOutput           = simpleOutput;
            sut.database_migrator.database.database_name = "DbName";
            sut.database_migrator.database.server_name   = "ServerName";
            sut.dropping_the_database                    = false;
            A.CallTo(() => sut.fakeKnownFolders.change_drop).Returns(MakeMigrationsFolder("change_drop", true, false));
            A.CallTo(() => sut.fakeKnownFolders.alter_database).Returns(MakeMigrationsFolder("alter", false, true));
            A.CallTo(() => sut.fakeKnownFolders.functions).Returns(MakeMigrationsFolder("functions", true, false));
            A.CallTo(() => sut.fakeVersionResolver.resolve_version()).Returns(NEW_DB_VERSION);
            A.CallTo(() => sut.database_migrator.get_current_version("")).WithAnyArguments().Returns("1");
            A.CallTo(() => sut.database_migrator.database.has_roundhouse_support_tables()).WithAnyArguments().Returns(true);
            return sut;
        }

        private MigrationsFolder MakeMigrationsFolder(string folderName, bool oneTime, bool everyTime)
        {
            return new DefaultMigrationsFolder(A.Dummy<WindowsFileSystemAccess>(), FOLDER_PATH, folderName, oneTime, everyTime, "friendly-" + folderName);
        }
    }
}
