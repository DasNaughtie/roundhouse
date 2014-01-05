using roundhouse.databases;
using roundhouse.databases.access;
using roundhouse.databases.sqlserver2000;

namespace roundhouse.unittests
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;

    using FakeItEasy;
    using NUnit.Framework;
    using roundhouse.consoles;
    using roundhouse.environments;
    using roundhouse.folders;
    using roundhouse.infrastructure.app;
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
        public bool CheckChangeDropFolderCreated = false;
        public StringBuilder CheckLogWritten     = new StringBuilder();
        public StringBuilder CheckWarningWritten = new StringBuilder();
        public StringBuilder CheckFilesCopied    = new StringBuilder();

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
            var sut = MakeTestableRoundhouseMigrationRunner(false);
            sut.run();
            StringAssert.Contains("Please press enter when ready to kick", sut.CheckLogWritten.ToString());
            Assert.AreEqual(true, sut.CheckChangeDropFolderCreated);
        }

        [Test]
        [TestCase(true, "This is a dry run", true)]
        [TestCase(false, "This is a dry run", false)]
        public void Run_WithOrWithoutDryRun_LogsAppropriateInfoActions(bool isDryRun, string testString, bool shouldContain)
        {
            var sut = MakeTestableRoundhouseMigrationRunner(isDryRun);
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
            var sut = MakeTestableRoundhouseMigrationRunner(false);
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
            var sut = MakeTestableRoundhouseMigrationRunner(true);
            sut.dropping_the_database = true;
            sut.run();
            StringAssert.Contains("would have removed database (DbName)", sut.CheckLogWritten.ToString());
            A.CallTo(() => sut.database_migrator.delete_database()).WithAnyArguments().MustHaveHappened();
            A.CallTo(() => sut.database_migrator.close_connection()).MustHaveHappened();
        }

        [Test]
        public void Run_WithDryRun_WillNotCreateTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true);
            sut.dont_create_the_database = false;
            sut.run();
            A.CallTo(() => sut.database_migrator.create_or_restore_database(A.Dummy<String>())).WithAnyArguments().MustHaveHappened();
        }

        [Test]
        public void Run_WithoutDryRun_WillCreateTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false);
            sut.dont_create_the_database = false;
            sut.run();
            StringAssert.Contains("Creating the database using", sut.CheckLogWritten.ToString());
            A.CallTo(() => sut.database_migrator.create_or_restore_database(A.Dummy<String>())).WithAnyArguments().MustHaveHappened();
        }

        [Test]
        [TestCase(RecoveryMode.Full)]
        [TestCase(RecoveryMode.Simple)]
        public void Run_WithoutDryRun_WillSetRecoveryMode(RecoveryMode recoveryMode)
        {

            var sut = MakeTestableRoundhouseMigrationRunner(false);
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

            var sut = MakeTestableRoundhouseMigrationRunner(true);
            sut.fakeConfiguration.RecoveryMode = recoveryMode;
            sut.run();
            StringAssert.Contains(expected, sut.CheckLogWritten.ToString());
            A.CallTo(() => sut.database_migrator.set_recovery_mode(recoveryMode == RecoveryMode.Simple)).MustHaveHappened();
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
            // TODO (PMO): Clean this up so it doesn't rely on stringAsserts, but rather A.CallTo(...
            var sut = new TestableRoundhouseMigrationRunner();
            sut.fakeConfiguration.DryRun = true;
            sut.run_in_a_transaction = runInTransaction;
            if (supportsTransaction)
            {
                sut.database_migrator.database = new SqlServerDatabase();
            }
            else
            {
                sut.database_migrator.database = new AccessDatabase();
            }
            sut.fakeDbMigrator.database.database_name = "DbName";
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
            var sut = MakeTestableRoundhouseMigrationRunner(false);
            sut.run_in_a_transaction = runInTransaction;
            if (supportsTransaction)
            {
                sut.database_migrator.database = new SqlServerDatabase();
            }
            else
            {
                sut.database_migrator.database = new AccessDatabase();
            }
            sut.run();
            StringAssert.DoesNotContain(notExpected, sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithDryRun_DoesRunRoundhouseSupportTasks()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true);
            sut.run();
            StringAssert.Contains("Would have run roundhouse support tasks on database DbName", sut.CheckLogWritten.ToString());
            A.CallTo(() => sut.fakeDbMigrator.run_roundhouse_support_tasks()).MustHaveHappened();
        }

        [Test]
        public void Run_WithoutDryRun_RunsSupportTasks()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false);
            sut.run();
            StringAssert.DoesNotContain("Would have run roundhouse support tasks on database DbName", sut.CheckLogWritten.ToString());
            A.CallTo(() => sut.fakeDbMigrator.run_roundhouse_support_tasks()).MustHaveHappened();
        }

        [Test]
        public void Run_WithDryRun_DoesCallVersionTheDatabaseBecauseItIsDryRunSafe()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true);
            sut.run();
            StringAssert.Contains("Would have migrated database DbName from version 1 to 2", sut.CheckLogWritten.ToString());
            A.CallTo(() => sut.database_migrator.get_current_version(REPO_PATH)).MustHaveHappened();
            A.CallTo(() => sut.database_migrator.version_the_database(REPO_PATH, NEW_DB_VERSION)).MustHaveHappened();
        }

        [Test]
        public void Run_WithoutDryRun_DoesInsertNewVersionRowIntoTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false);
            sut.run();
            StringAssert.Contains("Migrating DbName from version 1 to 2", sut.CheckLogWritten.ToString());
            A.CallTo(() => sut.database_migrator.get_current_version(REPO_PATH)).MustHaveHappened();
            A.CallTo(() => sut.database_migrator.version_the_database(REPO_PATH, NEW_DB_VERSION)).MustHaveHappened();
        }

        [Test]
        public void Run_WithDryRun_DoesLogAndTraverseFoldersBecauseRunSqlIsDryRunSafe()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true);
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
            var sut = MakeTestableRoundhouseMigrationRunner(false);
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
            var sut = MakeTestableRoundhouseMigrationRunner(false);
            sut.run();
            StringAssert.IsMatch("RoundhousE v([0-9.]*) has kicked your database \\(DbName\\)\\! You are now at version " + NEW_DB_VERSION + 
                "\\. All changes and backups can be found at \"" + FOLDER_PATH + "\\\\change_drop\"", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithDryRun_TellsYouYourDatabaseWouldHaveBeenKicked()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true);
            sut.run();
            StringAssert.IsMatch("-DryRun-RoundhousE v([0-9.]*) would have kicked your database \\(DbName\\)\\! You would be at version " + NEW_DB_VERSION
                + "\\. All changes and backups can be found at \"" + FOLDER_PATH + "\\\\change_drop\"", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithoutDryRunAndDatabaseDropRequested_DropsTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false);
            sut.dropping_the_database = true;
            sut.run();
            StringAssert.Contains("RoundhousE has removed database (DbName). All changes and backups can be found at \"" + FOLDER_PATH + "\\change_drop\"", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithDryRunAndDatabaseDropRequested_DoesntDropTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true);
            sut.dropping_the_database = true;
            sut.run();
            StringAssert.Contains("-DryRun-RoundhousE would have removed database (DbName). All changes and backups would be found at \"" + FOLDER_PATH + "\\change_drop\"", sut.CheckLogWritten.ToString());
        }

        private TestableRoundhouseMigrationRunner MakeTestableRoundhouseMigrationRunner(bool dryRun)
        {
            var sut                                   = new TestableRoundhouseMigrationRunner();
            sut.fakeConfiguration.DryRun              = dryRun;
            sut.fakeDbMigrator.database.database_name = "DbName";
            sut.dropping_the_database                 = false;
            A.CallTo(() => sut.fakeKnownFolders.change_drop)
                .Returns(MakeMigrationsFolder("change_drop", true, false));
            A.CallTo(() => sut.fakeKnownFolders.alter_database).Returns(MakeMigrationsFolder("alter", false, true));
            A.CallTo(() => sut.fakeKnownFolders.functions).Returns(MakeMigrationsFolder("functions", true, false));
            A.CallTo(() => sut.fakeVersionResolver.resolve_version()).Returns(NEW_DB_VERSION);
            A.CallTo(() => sut.fakeDbMigrator.get_current_version("")).WithAnyArguments().Returns("1");
            return sut;
        }

        private MigrationsFolder MakeMigrationsFolder(string folderName, bool oneTime, bool everyTime)
        {
            return new DefaultMigrationsFolder(A.Dummy<WindowsFileSystemAccess>(), FOLDER_PATH, folderName, oneTime, everyTime, "friendly-" + folderName);
        }
    }
}
