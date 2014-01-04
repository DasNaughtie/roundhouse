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
    using roundhouse.migrators;
    using NUnit.Framework;

    public class TestableDefaultDatabaseMigrator : DefaultDatabaseMigrator
    {
        // mocks
        public Database mockDatabase                               = A.Fake<Database>();
        public CryptographicService mockCryptoService              = A.Fake<CryptographicService>();
        public ConfigurationPropertyHolder configurationPropHolder = new DefaultConfiguration();

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
    }


    [TestFixture]
    public class DatabaseMigratorTests
    {
        [Test]
        public void CreateOrRestoreDatabase_WithoutDryRun_CreatesTheDatabase()
        {
            var sut = new TestableDefaultDatabaseMigrator();
            var created = sut.create_or_restore_database(A.Dummy<String>());
            Assert.AreEqual(true, created);
        }
    }
}
