using roundhouse.databases;
using roundhouse.databases.access;
using roundhouse.databases.sqlserver2000;

namespace roundhouse.unittests
{
    using FakeItEasy;
    using NUnit.Framework;
    using roundhouse.consoles;
    using roundhouse.environments;
    using roundhouse.folders;
    using roundhouse.infrastructure.filesystem;
    using roundhouse.infrastructure.logging;
    using roundhouse.migrators;
    using roundhouse.resolvers;
    using roundhouse.runners;
    using System.Text;

    public class TestableRoundhouseMigrationRunner : RoundhouseMigrationRunner
    {
        public Logger mockLogger                                   = A.Fake<Logger>();
        public static DatabaseMigrator mockDbMigrator              = A.Fake<DatabaseMigrator>();
        public static DefaultConfiguration mockConfiguration       = A.Fake<DefaultConfiguration>();
        public static DefaultEnvironment mockEnvironment           = A.Fake<DefaultEnvironment>();
        public static KnownFolders mockKnownFolders                = A.Fake<KnownFolders>();
        public static WindowsFileSystemAccess mockFileSystemAccess = A.Fake<WindowsFileSystemAccess>();
        public static VersionResolver mockVersionResolver          = A.Fake<VersionResolver>();

        // Quick stub checks
        public bool CheckChangeDropFolderCreated = false;
        public StringBuilder CheckLogWritten     = new StringBuilder();
        public StringBuilder CheckWarningWritten = new StringBuilder();

        public TestableRoundhouseMigrationRunner()
            : base("repo_path",
                mockEnvironment,
                mockKnownFolders,
                mockFileSystemAccess,
                mockDbMigrator, 
                mockVersionResolver,
                false,
                false,
                false,
                false,
                true,
                mockConfiguration)
        {
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
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = false;
            sut.run();
            StringAssert.Contains("Please press enter when ready to kick", sut.CheckLogWritten.ToString());
            Assert.AreEqual(true, sut.CheckChangeDropFolderCreated);
        }

        [Test]
        [TestCase(true, "This is a dry run", true)]
        [TestCase(false, "This is a dry run", false)]
        public void Run_WithOrWithoutDryRun_LogsAppropriateInfoActions(bool isDryRun, string testString, bool shouldContain)
        {
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = isDryRun;
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
        public void Run_WithDryRun_WillNotDropDatabase()
        {
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = true;
            sut.dropping_the_database = true;
            sut.run();
            StringAssert.Contains("would have removed database", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithoutDryRun_WillDropDatabase()
        {
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = false;
            sut.dropping_the_database = true;
            sut.run();
            A.CallTo(() => sut.database_migrator.delete_database()).WithAnyArguments().MustHaveHappened();
            StringAssert.Contains("has removed database", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithDryRun_WillNotCreateTheDatabase()
        {
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = true;
            sut.dont_create_the_database = false;
            sut.run();
            StringAssert.Contains("Would have created the database", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithoutDryRun_WillCreateTheDatabase()
        {
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = false;
            sut.dont_create_the_database = false;
            sut.run();
            StringAssert.Contains("Creating the database using", sut.CheckLogWritten.ToString());
        }

        [Test]
        [TestCase(RecoveryMode.Full, "Would have set the database recovery mode to Full on database DbName")]
        [TestCase(RecoveryMode.Simple, "Would have set the database recovery mode to Simple on database DbName")]
        public void Run_WithDryRun_WillNotSetRecoveryMode(RecoveryMode recoveryMode, string expected)
        {
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = true;
            TestableRoundhouseMigrationRunner.mockConfiguration.RecoveryMode = recoveryMode;
            TestableRoundhouseMigrationRunner.mockDbMigrator.database.database_name = "DbName";
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
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = true;
            sut.run_in_a_transaction = runInTransaction;
            if (supportsTransaction)
            {
                sut.database_migrator.database = new SqlServerDatabase();
            }
            else
            {
                sut.database_migrator.database = new AccessDatabase();
            }
            TestableRoundhouseMigrationRunner.mockDbMigrator.database.database_name = "DbName";
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
        [TestCase(true, true, null, "Would have began a transaction on database DbName")]
        [TestCase(true, false, null, "Would have began a transaction on database DbName")]
        [TestCase(false, true, null, "Would have began a transaction on database DbName")]
        [TestCase(false, false, null, "Would have began a transaction on database DbName")]
        public void Run_WithoutDryRunAndTransactionRequestedAndDbThatSupportsTransactions_WillBeginTransaction(
            bool runInTransaction,
            bool supportsTransaction,
            string expected,
            string notExpected)
        {
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = false;
            sut.run_in_a_transaction = runInTransaction;
            if (supportsTransaction)
            {
                sut.database_migrator.database = new SqlServerDatabase();
            }
            else
            {
                sut.database_migrator.database = new AccessDatabase();
            }
            TestableRoundhouseMigrationRunner.mockDbMigrator.database.database_name = "DbName";
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
        public void Run_WithDryRun_DoesntRunSupportTasks()
        {
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = true;
            TestableRoundhouseMigrationRunner.mockDbMigrator.database.database_name = "DbName";
            sut.run();
            StringAssert.Contains("Would have run roundhouse support tasks on database DbName", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithoutDryRun_RunsSupportTasks()
        {
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = false;
            TestableRoundhouseMigrationRunner.mockDbMigrator.database.database_name = "DbName";
            sut.run();
            StringAssert.DoesNotContain("Would have run roundhouse support tasks on database DbName", sut.CheckLogWritten.ToString());
            // This line doesn't prove much becauset he mock is static and has history from the other tests.
            // A.CallTo(() => TestableRoundhouseMigrationRunner.mockDbMigrator.run_roundhouse_support_tasks()).MustHaveHappened();
        }

        [Test]
        public void Run_WithDryRun_DoesNotInsertNewVersionRowIntoTheDatabase()
        {
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = true;
            TestableRoundhouseMigrationRunner.mockDbMigrator.database.database_name = "DbName";
            A.CallTo(() => TestableRoundhouseMigrationRunner.mockVersionResolver.resolve_version()).Returns("2");
            A.CallTo(() => TestableRoundhouseMigrationRunner.mockDbMigrator.get_current_version("")).WithAnyArguments().Returns("1");
            sut.run();
            StringAssert.Contains("Would have migrated database DbName from version 1 to 2", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithoutDryRun_DoesInsertNewVersionRowIntoTheDatabase()
        {
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = false;
            TestableRoundhouseMigrationRunner.mockDbMigrator.database.database_name = "DbName";
            A.CallTo(() => TestableRoundhouseMigrationRunner.mockVersionResolver.resolve_version()).Returns("2");
            A.CallTo(() => TestableRoundhouseMigrationRunner.mockDbMigrator.get_current_version("")).WithAnyArguments().Returns("1");
            sut.run();
            StringAssert.Contains("Migrating DbName from version 1 to 2", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithDryRun_DoesNotLogAndTraverseFolders()
        {
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = true;
            A.CallTo(() => TestableRoundhouseMigrationRunner.mockKnownFolders.alter_database).Returns(MakeMigrationsFolder("alter", false, true));
            A.CallTo(() => TestableRoundhouseMigrationRunner.mockKnownFolders.functions).Returns(MakeMigrationsFolder("functions", true, false));
            sut.run();
            StringAssert.Contains("Would have been looking for friendly-alter scripts in \"folderpath\\alter\". These scripts would be run every time", sut.CheckLogWritten.ToString());
            StringAssert.Contains("Would have been looking for friendly-functions scripts in \"folderpath\\functions\". These would be one time only scripts", sut.CheckLogWritten.ToString());
        }

        [Test]
        public void Run_WithoutDryRun_DoesLogAndTraverseFolders()
        {
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = false;
            A.CallTo(() => TestableRoundhouseMigrationRunner.mockKnownFolders.alter_database).Returns(MakeMigrationsFolder("alter", false, true));
            A.CallTo(() => TestableRoundhouseMigrationRunner.mockKnownFolders.functions).Returns(MakeMigrationsFolder("functions", true, false));
            sut.run();
            StringAssert.Contains("Looking for friendly-alter scripts in \"folderpath\\alter\". These scripts will run every time", sut.CheckLogWritten.ToString());
            StringAssert.Contains("Looking for friendly-functions scripts in \"folderpath\\functions\". These should be one time only scripts", sut.CheckLogWritten.ToString());
        }


        private MigrationsFolder MakeMigrationsFolder(string folderName, bool oneTime, bool everyTime)
        {
            return new DefaultMigrationsFolder(A.Dummy<WindowsFileSystemAccess>(), "folderpath", folderName, oneTime, everyTime, "friendly-" + folderName);
        }
    }
}
