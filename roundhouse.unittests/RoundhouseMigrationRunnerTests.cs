using roundhouse.databases;
using roundhouse.databases.access;
using roundhouse.databases.sqlserver2000;

namespace roundhouse.unittests
{
    using System;

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

    }

    [TestFixture]
    public class RoundhouseMigrationRunnerTests
    {
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
            A.CallTo(() => sut.database_migrator.delete_database()).WithAnyArguments().MustHaveHappened();
            StringAssert.Contains("has removed database", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithDryRun_WillNotCreateTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true);
            sut.dont_create_the_database = false;
            sut.run();
            StringAssert.Contains("Would have created the database", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithoutDryRun_WillCreateTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false);
            sut.dont_create_the_database = false;
            sut.run();
            StringAssert.Contains("Creating the database using", sut.CheckLogWritten.ToString());
        }

        [Test]
        [TestCase(RecoveryMode.Full, "Would have set the database recovery mode to Full on database DbName")]
        [TestCase(RecoveryMode.Simple, "Would have set the database recovery mode to Simple on database DbName")]
        public void Run_WithDryRun_WillNotSetRecoveryMode(RecoveryMode recoveryMode, string expected)
        {

            var sut = MakeTestableRoundhouseMigrationRunner(true);
            sut.fakeConfiguration.RecoveryMode = recoveryMode;
            sut.run();
            StringAssert.Contains(expected, sut.CheckLogWritten.ToString());
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
        public void Run_WithDryRun_DoesntRunSupportTasks()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true);
            sut.run();
            StringAssert.Contains("Would have run roundhouse support tasks on database DbName", sut.CheckLogWritten.ToString());
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
        public void Run_WithDryRun_DoesNotInsertNewVersionRowIntoTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true);
            sut.run();
            StringAssert.Contains("Would have migrated database DbName from version 1 to 2", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithoutDryRun_DoesInsertNewVersionRowIntoTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false);
            sut.run();
            StringAssert.Contains("Migrating DbName from version 1 to 2", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithDryRun_DoesNotLogAndTraverseFolders()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true);
            sut.run();
            StringAssert.Contains("Would have been looking for friendly-alter scripts in \"folderpath\\alter\". These scripts would be run every time", sut.CheckLogWritten.ToString());
            StringAssert.Contains("Would have been looking for friendly-functions scripts in \"folderpath\\functions\". These would be one time only scripts", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithoutDryRun_DoesLogAndTraverseFolders()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false);
            sut.run();
            StringAssert.Contains("Looking for friendly-alter scripts in \"folderpath\\alter\". These scripts will run every time", sut.CheckLogWritten.ToString());
            StringAssert.Contains("Looking for friendly-functions scripts in \"folderpath\\functions\". These should be one time only scripts", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithoutDryRun_TellsYouYourDatabaseWasKicked()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false);
            sut.run();
            StringAssert.IsMatch("RoundhousE v([0-9.]*) has kicked your database \\(DbName\\)\\! You are now at version 2\\. All changes and backups can be found at \"folderpath\\\\change_drop\"", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithDryRun_TellsYouYourDatabaseWouldHaveBeenKicked()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true);
            sut.run();
            StringAssert.IsMatch("-DryRun-RoundhousE v([0-9.]*) would have kicked your database \\(DbName\\)\\! You would be at version 2\\. All changes and backups can be found at \"folderpath\\\\change_drop\"", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithoutDryRunAndDatabaseDropRequested_DropsTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(false);
            sut.dropping_the_database = true;
            sut.run();
            StringAssert.Contains("RoundhousE has removed database (DbName). All changes and backups can be found at \"folderpath\\change_drop\"", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithDryRunAndDatabaseDropRequested_DoesntDropTheDatabase()
        {
            var sut = MakeTestableRoundhouseMigrationRunner(true);
            sut.dropping_the_database = true;
            sut.run();
            StringAssert.Contains("-DryRun-RoundhousE would have removed database (DbName). All changes and backups would be found at \"folderpath\\change_drop\"", sut.CheckLogWritten.ToString());
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
            A.CallTo(() => sut.fakeVersionResolver.resolve_version()).Returns("2");
            A.CallTo(() => sut.fakeDbMigrator.get_current_version("")).WithAnyArguments().Returns("1");
            return sut;
        }

        private MigrationsFolder MakeMigrationsFolder(string folderName, bool oneTime, bool everyTime)
        {
            return new DefaultMigrationsFolder(A.Dummy<WindowsFileSystemAccess>(), "folderpath", folderName, oneTime, everyTime, "friendly-" + folderName);
        }
    }
}
