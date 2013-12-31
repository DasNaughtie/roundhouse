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
            CheckLogWritten.Append(string.Format(message, args));
        }
        protected override void log_info_event_on_bound_logger(string message)
        {
            CheckLogWritten.Append(string.Format(message));
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
        public void Run_WithDryRunConfiguration_LogsThatItIsADryRun()
        {
            var sut = new TestableRoundhouseMigrationRunner();
            TestableRoundhouseMigrationRunner.mockConfiguration.DryRun = true;
            sut.run();
            StringAssert.Contains("This is a dry run", sut.CheckLogWritten.ToString());
            Assert.AreEqual(false, sut.CheckChangeDropFolderCreated);
        }
    }
}
