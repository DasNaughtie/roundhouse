namespace roundhouse.unittests
{
    using System.Text;

    using FakeItEasy;

    using NUnit.Framework;

    using roundhouse.consoles;
    using roundhouse.environments;
    using roundhouse.folders;
    using roundhouse.infrastructure.extensions;
    using roundhouse.infrastructure.filesystem;
    using roundhouse.infrastructure.logging;
    using roundhouse.migrators;
    using roundhouse.resolvers;
    using roundhouse.runners;

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
            return this.mockLogger;
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

    }
}
