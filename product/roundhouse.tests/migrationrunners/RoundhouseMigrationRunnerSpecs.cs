namespace roundhouse.tests.infrastructure.containers
{
    using System;
    using bdddoc.core;
    using consoles;
    using developwithpassion.bdd.contexts;
    using developwithpassion.bdd.mbunit;
    using developwithpassion.bdd.mbunit.standard;
    using developwithpassion.bdd.mbunit.standard.observations;
    using environments;
    using migrators;
    using Rhino.Mocks;

    using roundhouse.cryptography;
    using roundhouse.databases.sqlserver;
    using roundhouse.folders;
    using roundhouse.infrastructure.app;
    using roundhouse.infrastructure.app.builders;
    using roundhouse.infrastructure.containers;
    using roundhouse.infrastructure.filesystem;
    using roundhouse.infrastructure.logging;
    using roundhouse.infrastructure.logging.custom;
    using roundhouse.resolvers;
    using roundhouse.runners;

    using StructureMap;
    using Container=roundhouse.infrastructure.containers.Container;
    using Environment = roundhouse.environments.Environment;

    public class TestableRoundhouseMigrationRunner : RoundhouseMigrationRunner
    {
        MockRepository _mock_repo = new MockRepository();

        private Logger _mock_logger;

        public TestableRoundhouseMigrationRunner() : base("repo_path", 
            new DefaultEnvironment(new DefaultConfiguration()), 
            KnownFoldersBuilder.build(new WindowsFileSystemAccess(), new DefaultConfiguration()),
            new WindowsFileSystemAccess(), 
            new DefaultDatabaseMigrator(new SqlServerDatabase(), new MD5CryptographicService(), new DefaultConfiguration()), 
            VersionResolverBuilder.build(new WindowsFileSystemAccess(), new DefaultConfiguration()),
            false,
            false,
            false,
            false,
            true,
            new DefaultConfiguration())
        {
            _mock_logger = _mock_repo.DynamicMock<Logger>();
        }

        public TestableRoundhouseMigrationRunner(
            string repository_path,
            Environment environment,
            KnownFolders known_folders,
            FileSystemAccess file_system,
            DatabaseMigrator database_migrator,
            VersionResolver version_resolver,
            bool silent,
            bool dropping_the_database,
            bool dont_create_the_database,
            bool run_in_a_transaction,
            bool use_simple_recovery,
            ConfigurationPropertyHolder configuration)
            : base(
                repository_path,
                environment,
                known_folders,
                file_system,
                database_migrator,
                version_resolver,
                silent,
                dropping_the_database,
                dont_create_the_database,
                run_in_a_transaction,
                use_simple_recovery,
                configuration)
        {
            _mock_logger = _mock_repo.DynamicMock<Logger>();
        }

        protected override Logger get_bound_logger()
        {
            return _mock_logger;
        }
    }

    public class RoundhouseMigrationRunnerSpecs
    {
        public abstract class concern_for_migration_runner : observations_for_a_sut_without_a_contract<TestableRoundhouseMigrationRunner>
        {
            protected static object result;
            protected static DefaultEnvironment environment;

            context c = () => {
                            environment = new DefaultEnvironment(new DefaultConfiguration {EnvironmentName = "TEST"});
                        };
        }

        [Concern(typeof(TestableRoundhouseMigrationRunner))]
        public class when_running_default : concern_for_migration_runner
        {
            context c = () => { };
            because b = () => { };

            [Observation]
            public void if_run_it_can_create_runner()
            {
                sut.run();
            }


        }
        

     
    }
}