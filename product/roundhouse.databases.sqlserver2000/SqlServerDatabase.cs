using roundhouse.infrastructure.logging;

namespace roundhouse.databases.sqlserver2000
{
    using System.Text.RegularExpressions;
    using infrastructure.app;
    using infrastructure.extensions;

    public class SqlServerDatabase : AdoNetDatabase
    {
        private string connect_options = "Integrated Security";

        public override string sql_statement_separator_regex_pattern
        {
            get { return @"(?<KEEP1>^(?:.)*(?:-{2}).*$)|(?<KEEP1>/{1}\*{1}[\S\s]*?\*{1}/{1})|(?<KEEP1>'{1}(?:[^']|\n[^'])*?'{1})|(?<KEEP1>\s)(?<BATCHSPLITTER>GO)(?<KEEP2>\s)|(?<KEEP1>\s)(?<BATCHSPLITTER>GO)(?<KEEP2>$)"; }
        }

        public override void initialize_connections(ConfigurationPropertyHolder configuration_property_holder)
        {
            if (!string.IsNullOrEmpty(connection_string))
            {
                string[] parts = connection_string.Split(';');
                foreach (string part in parts)
                {
                    if (string.IsNullOrEmpty(server_name) && (part.to_lower().Contains("server") || part.to_lower().Contains("data source")))
                    {
                        server_name = part.Substring(part.IndexOf("=") + 1);
                    }

                    if (string.IsNullOrEmpty(database_name) && (part.to_lower().Contains("initial catalog") || part.to_lower().Contains("database")))
                    {
                        database_name = part.Substring(part.IndexOf("=") + 1);
                    }
                }

                if (!connection_string.to_lower().Contains(connect_options.to_lower()))
                {
                    connect_options = string.Empty;
                    foreach (string part in parts)
                    {
                        if (!part.to_lower().Contains("server") && !part.to_lower().Contains("data source") && !part.to_lower().Contains("initial catalog") &&
                            !part.to_lower().Contains("database"))
                        {
                            connect_options += part + ";";
                        }
                    }
                }
            }


            if (connect_options == "Integrated Security")
            {
                connect_options = "Integrated Security=SSPI;";
            }

            if (string.IsNullOrEmpty(connection_string))
            {
                connection_string = build_connection_string(server_name, database_name, connect_options);
            }

            configuration_property_holder.ConnectionString = connection_string;

            set_provider();
            if (string.IsNullOrEmpty(admin_connection_string))
            {
                admin_connection_string = Regex.Replace(connection_string, "initial catalog=.*?;", "initial catalog=master;", RegexOptions.IgnoreCase);
                admin_connection_string = Regex.Replace(admin_connection_string, "database=.*?;", "database=master;", RegexOptions.IgnoreCase);
            }
            configuration_property_holder.ConnectionStringAdmin = admin_connection_string;

            //set_repository(configuration_property_holder);
        }

        public override void set_provider()
        {
            provider = "System.Data.SqlClient";
        }

        private static string build_connection_string(string server_name, string database_name, string connection_options)
        {
            return string.Format("Server={0};initial catalog={1};{2}", server_name, database_name, connection_options);
        }

        public override string generate_database_specific_script()
        {
            return string.Empty;
        }

        public override string run_database_specific_tasks()
        {

            Log.bound_to(this).log_a_debug_event_containing("FUTURE ENHANCEMENT: Should create a user by the name of RoundhousE.");
            //this.roundhouse_schema_name;
            //TODO: Create user

            var sql = generate_database_specific_script();

            //run_sql(set_recovery_mode_script(simple)
            return sql;
        }

        public override bool has_roundhouse_support_tables()
        {
            var sql = "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[RoundhousE].[ScriptsRun]') AND type in (N'U')) " +
            "    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[RoundhousE].[ScriptsRunErrors]') AND type in (N'U'))" +
            "        IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[RoundhousE].[Version]') AND type in (N'U'))" +
            "            SELECT 1 AS HasRoundhousESupportTables " +
            "ELSE " +
            "    SELECT 0 AS HasRoundhousESupportTables";
            var result = run_sql_scalar(sql, ConnectionType.Default) as int?;
            return result.HasValue && result == 1;
        }

        public override string create_database_script()
        {
            return string.Format(
                @"
                        DECLARE @Created bit
                        SET @Created = 0
                        IF NOT EXISTS(SELECT * FROM sys.databases WHERE [name] = '{0}') 
                         BEGIN 
                            CREATE DATABASE [{0}] 
                            SET @Created = 1
                         END

                        SELECT @Created 
                        ",
                database_name);
        }

        public override string set_recovery_mode_script(bool simple)
        {
            return string.Format(
                @"USE master 
                   ALTER DATABASE [{0}] SET RECOVERY {1}
                    ",
                database_name, simple ? "SIMPLE" : "FULL");
        }

        public override string restore_database_script(string restore_from_path, string custom_restore_options)
        {
            string restore_options = string.Empty;
            if (!string.IsNullOrEmpty(custom_restore_options))
            {
                restore_options = custom_restore_options.to_lower().StartsWith(",") ? custom_restore_options : ", " + custom_restore_options;
            }

            return string.Format(
                @"USE master 
                        ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                        
                        RESTORE DATABASE [{0}]
                        FROM DISK = N'{1}'
                        WITH NOUNLOAD
                        , STATS = 10
                        , RECOVERY
                        , REPLACE
                        {2};

                        ALTER DATABASE [{0}] SET MULTI_USER;
                        ",
                database_name, restore_from_path,
                restore_options
                );
        }

        public override string delete_database_script()
        {
            return string.Format(
                @"USE master 
                        IF EXISTS(SELECT * FROM sysdatabases WHERE [name] = '{0}') 
                        BEGIN 
                            ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE                            
                            DROP DATABASE [{0}] 
                        END",
                database_name);
        }
    }
}