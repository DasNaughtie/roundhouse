using System.Collections.Generic;

namespace roundhouse.migrators
{
    using System;
    using cryptography;
    using databases;
    using infrastructure.app;
    using infrastructure.app.tokens;
    using infrastructure.extensions;
    using infrastructure.logging;
    using sqlsplitters;
    using Environment = roundhouse.environments.Environment;

    public class DefaultDatabaseMigrator : DatabaseMigrator
    {
        public Database database { get; set; }

        public bool is_running_a_dry_run { get; set; }

        protected CryptographicService crypto_provider;
        protected ConfigurationPropertyHolder configuration;
        protected bool restoring_database;
        protected string restore_path;
        protected string custom_restore_options;
        protected string output_path;
        protected bool throw_error_on_one_time_script_changes;
        protected bool running_in_a_transaction;
        protected bool is_running_all_any_time_scripts;


        public DefaultDatabaseMigrator(Database database, CryptographicService crypto_provider, ConfigurationPropertyHolder configuration)
        {
            this.database = database;
            this.crypto_provider = crypto_provider;
            this.configuration = configuration;
            restoring_database = configuration.Restore;
            restore_path = configuration.RestoreFromPath;
            custom_restore_options = configuration.RestoreCustomOptions;
            output_path = configuration.OutputPath;
            throw_error_on_one_time_script_changes = !configuration.WarnOnOneTimeScriptChanges;
            is_running_all_any_time_scripts = configuration.RunAllAnyTimeScripts;
        }

        public void initialize_connections()
        {
            database.initialize_connections(configuration);
        }

        public void open_admin_connection()
        {
            database.open_admin_connection();
        }

        public void close_admin_connection()
        {
            database.close_admin_connection();
        }

        public void open_connection(bool with_transaction)
        {
            running_in_a_transaction = with_transaction;
            database.open_connection(with_transaction);
        }

        public void close_connection()
        {
            database.close_connection();
        }

        protected virtual void log_info_event_on_bound_logger(string message, params object[] args)
        {
            Log.bound_to(this).log_an_info_event_containing(message, args);
        }

        protected virtual void log_debug_event_on_bound_logger(string message, params object[] args)
        {
            Log.bound_to(this).log_a_debug_event_containing(message, args);
        }

        protected virtual void log_warning_event_on_bound_logger(string message, params object[] args)
        {
            Log.bound_to(this).log_a_warning_event_containing(message, args);
        }

        public bool create_or_restore_database(string custom_create_database_script)
        {
            var database_created = false;

            log_what_we_are_about_to_do_create_or_restore(custom_create_database_script);

            if (configuration.DryRun == false)
            {
                database_created = database.create_database_if_it_doesnt_exist(custom_create_database_script);
            }

            if (restoring_database)
            {
                database_created = false;
                string custom_script = custom_restore_options;
                if (!configuration.DisableTokenReplacement)
                {
                    custom_script = TokenReplacer.replace_tokens(configuration, custom_script);
                }
                restore_database(restore_path, custom_script);
            }

            return database_created;
        }

        private void log_what_we_are_about_to_do_create_or_restore(string custom_create_database_script)
        {
            if (configuration.DryRun)
            {
                if (string.IsNullOrEmpty(custom_create_database_script))
                {
                    this.log_info_event_on_bound_logger(
                        "-DryRun-Would have created {0} database on {1} server (if it didn't exist).",
                        this.database.database_name,
                        this.database.server_name);
                }
                else
                {
                    this.log_info_event_on_bound_logger(
                        "-DryRun-Would have created {0} database on {1} server with custom script.",
                        this.database.database_name,
                        this.database.server_name);
                }
            }
            else
            {
                if (string.IsNullOrEmpty(custom_create_database_script))
                {
                    log_info_event_on_bound_logger("Creating {0} database on {1} server if it doesn't exist.", database.database_name, database.server_name);
                }
                else
                {
                    log_info_event_on_bound_logger("Creating {0} database on {1} server with custom script.", database.database_name, database.server_name);
                }

            }
        }

        public void backup_database_if_it_exists()
        {
            if (configuration.DryRun)
            {
                log_info_event_on_bound_logger(
                    "-DryRun-Would have attempted a backup on {0} database on {1} server.",
                    database.database_name,
                    database.server_name);
            }
            else
            {
                log_info_event_on_bound_logger(
                    "Backing up {0} database on {1} server.",
                    database.database_name,
                    database.server_name);
                database.backup_database(output_path);
            }
        }

        public void restore_database(string restore_from_path, string restore_options)
        {
            if (configuration.DryRun)
            {
                log_info_event_on_bound_logger("-DryRun-Would have restored {0} database on {1} server from path {2}.", database.database_name, database.server_name, restore_from_path);
            }
            else
            {
                log_info_event_on_bound_logger("Restoring {0} database on {1} server from path {2}.", database.database_name, database.server_name, restore_from_path);
                database.restore_database(restore_from_path, restore_options);
            }
        }

        public string set_recovery_mode(bool simple)
        {
            if (configuration.DryRun)
            {
                log_info_event_on_bound_logger("-DryRun-Would have set recovery mode to '{0}' for database {1}.", simple ? "Simple" : "Full", database.database_name);
            }
            else
            {
                //database.open_connection(false);
                log_info_event_on_bound_logger("Setting recovery mode to '{0}' for database {1}.", simple ? "Simple" : "Full", database.database_name);
                database.set_recovery_mode(simple);
                //database.close_connection();
            }
            return database.generate_recovery_mode_script();
        }

        public void run_roundhouse_support_tasks()
        {
            if (running_in_a_transaction)
            {
                database.close_connection();
                database.open_connection(false);
            }

            if (configuration.DryRun)
            {
                log_info_event_on_bound_logger("-DryRun-Would run database type specific tasks.");
                //database.run_database_specific_tasks();
                log_info_event_on_bound_logger(" -> Would create [{0}] table if it didn't exist.", database.version_table_name);
                log_info_event_on_bound_logger(" -> Would create [{0}] table if it didn't exist.", database.scripts_run_table_name);
                log_info_event_on_bound_logger(" -> Would create [{0}] table if it didn't exist.", database.scripts_run_errors_table_name);
                // TODO (PMO): Make sure the RoundHouse support tasks make it into the drop folder
            }
            else
            {
                log_info_event_on_bound_logger("Running database type specific tasks.");
                database.run_database_specific_tasks();
                log_info_event_on_bound_logger(" -> Creating [{0}] table if it doesn't exist.", database.version_table_name);
                log_info_event_on_bound_logger(" -> Creating [{0}] table if it doesn't exist.", database.scripts_run_table_name);
                log_info_event_on_bound_logger(" -> Creating [{0}] table if it doesn't exist.", database.scripts_run_errors_table_name);
                database.create_or_update_roundhouse_tables();
            }

            if (running_in_a_transaction)
            {
                database.close_connection();
                database.open_connection(true);
                //transfer_to_database_for_changes();
            }
        }

        public string get_current_version(string repository_path)
        {
            string current_version = database.get_version(repository_path);

            if (string.IsNullOrEmpty(current_version))
            {
                current_version = "0";
            }

            return current_version;
        }

        public string delete_database()
        {
            string delete_script;
            if (configuration.DryRun)
            {
                log_info_event_on_bound_logger(" -> Would have deleted {0} database on {1} server if it existed.", database.database_name, database.server_name);
                delete_script = database.delete_database_script();
            }
            else
            {
                log_info_event_on_bound_logger(" -> Deleting {0} database on {1} server if it exists.", database.database_name, database.server_name);
                delete_script = database.delete_database_if_it_exists();
            }

            return delete_script;
        }

        public long version_the_database(string repository_path, string repository_version) {
            if (configuration.DryRun)
            {
                log_info_event_on_bound_logger(" -> Would version {0} database with version {1} based on path \"{2}\".", database.database_name, repository_version, repository_path);
                // TODO (PMO): Make it return a realistic version number
                return 0;
            }
            else
            {
                log_info_event_on_bound_logger(" -> Versioning {0} database with version {1} based on path \"{2}\".", database.database_name, repository_version, repository_path);
                return database.insert_version_and_get_version_id(repository_path, repository_version);
            }
        }

        public bool run_sql(string sql_to_run, string script_name, bool run_this_script_once, bool run_this_script_every_time, long version_id, Environment environment, string repository_version, string repository_path, ConnectionType connection_type)
        {
            bool this_sql_ran = false;

            handle_one_time_already_run(sql_to_run, script_name, run_this_script_once, repository_version, repository_path);

            // run once so we don't write to the screen twice
            var good_environment_file   = this_is_an_environment_file_and_its_in_the_right_environment(script_name, environment);
            var is_not_environment_file = this_is_not_an_environment_file(script_name);
            var script_should_run       = this_script_should_run(script_name, sql_to_run, run_this_script_once, run_this_script_every_time);

            if ((good_environment_file || is_not_environment_file) && script_should_run)
            {
                run_all_the_sql_statements(sql_to_run, script_name, run_this_script_once, version_id, repository_version, repository_path, connection_type);
                this_sql_ran = true;
            }
            else if (is_not_environment_file)
            {
                // exclude good_environment files because they already printed to the screen that the file was skipped.
                log_info_event_on_bound_logger("    Skipped {0} - {1}.", script_name, run_this_script_once ? "One time script" : "No changes were found to run");
            }

            return this_sql_ran;
        }

        protected void run_all_the_sql_statements(
            string sql_to_run,
            string script_name,
            bool run_this_script_once,
            long version_id,
            string repository_version,
            string repository_path,
            ConnectionType connection_type)
        {
            if (configuration.DryRun)
            {
                log_info_event_on_bound_logger(
                    " -> Would have run {0} on {1} - {2}.",
                    script_name,
                    database.server_name,
                    database.database_name);

                record_script_in_scripts_run_table_is_dry_run_safe(script_name, sql_to_run, run_this_script_once, version_id);
            }
            else
            {
                log_info_event_on_bound_logger(
                    " -> Running {0} on {1} - {2}.",
                    script_name,
                    database.server_name,
                    database.database_name);

                foreach (var sql_statement in get_statements_to_run(sql_to_run))
                {
                    run_sql_in_database(
                        sql_to_run,
                        script_name,
                        repository_version,
                        repository_path,
                        connection_type,
                        sql_statement);
                }
                record_script_in_scripts_run_table_is_dry_run_safe(script_name, sql_to_run, run_this_script_once, version_id);
            }
        }

        private void run_sql_in_database(
            string sql_to_run,
            string script_name,
            string repository_version,
            string repository_path,
            ConnectionType connection_type,
            string sql_statement)
        {
            try
            {
                database.run_sql(sql_statement, connection_type);
            }
            catch (Exception ex)
            {
                Log.bound_to(this)
                    .log_an_error_event_containing(
                        "Error executing file '{0}': statement running was '{1}'",
                        script_name,
                        sql_statement);
                database.rollback();

                record_script_in_scripts_run_errors_table_is_dry_run_safe(
                    script_name,
                    sql_to_run,
                    sql_statement,
                    ex.Message,
                    repository_version,
                    repository_path);
                database.close_connection();
                throw;
            }
        }

        protected void handle_one_time_already_run(
            string sql_to_run,
            string script_name,
            bool run_this_script_once,
            string repository_version,
            string repository_path)
        {
            if (this_is_a_one_time_script_that_has_changes_but_has_already_been_run(
                script_name,
                sql_to_run,
                run_this_script_once))
            {
                if (throw_error_on_one_time_script_changes)
                {
                    handle_error_on_one_time_script_change(sql_to_run, script_name, repository_version, repository_path);
                }
                log_warning_event_on_bound_logger("{0} is a one time script that has changed since it was run.", script_name);
            }
        }

        private void handle_error_on_one_time_script_change(
            string sql_to_run,
            string script_name,
            string repository_version,
            string repository_path)
        {
            database.rollback();
            string error_message =
                string.Format(
                    "{0} has changed since the last time it was run. By default this is not allowed - scripts that run once should never change. To change this behavior to a warning, please set warnOnOneTimeScriptChanges to true and run again. Stopping execution.",
                    script_name);
            record_script_in_scripts_run_errors_table_is_dry_run_safe(
                script_name,
                sql_to_run,
                sql_to_run,
                error_message,
                repository_version,
                repository_path);
            database.close_connection();
            throw new Exception(error_message);
        }

        public IEnumerable<string> get_statements_to_run(string sql_to_run)
        {
            IList<string> sql_statements = new List<string>();

            if (database.split_batch_statements)
            {
                foreach (var sql_statement in StatementSplitter.split_sql_on_regex_and_remove_empty_statements(sql_to_run, database.sql_statement_separator_regex_pattern))
                {
                    sql_statements.Add(sql_statement);
                }
            }
            else
            {
                sql_statements.Add(sql_to_run);
            }

            return sql_statements;
        }

        public void record_script_in_scripts_run_table_is_dry_run_safe(string script_name, string sql_to_run, bool run_this_script_once, long version_id)
        {
            if (configuration.DryRun)
            {
                log_info_event_on_bound_logger(" -> Would record {0} script ran on {1} - {2} in the {3} table.", script_name, database.server_name, database.database_name, database.scripts_run_table_name);
            }
            else
            {
                log_debug_event_on_bound_logger(" -> Recording {0} script ran on {1} - {2}.", script_name, database.server_name, database.database_name);
                database.insert_script_run(script_name, sql_to_run, create_hash(sql_to_run), run_this_script_once, version_id);
            }
        }

        public void record_script_in_scripts_run_errors_table_is_dry_run_safe(string script_name, string sql_to_run, string sql_erroneous_part, string error_message, string repository_version, string repository_path)
        {
            if (configuration.DryRun)
            {
                log_info_event_on_bound_logger(" -> Would have recorded {0} script ran with error on {1} - {2} in the {3} table.",
                    script_name, database.server_name, database.database_name,
                    database.scripts_run_errors_table_name);
            }
            else
            {
                log_debug_event_on_bound_logger(" -> Recording {0} script ran with error on {1} - {2}.", script_name, database.server_name, database.database_name);
                database.insert_script_run_error(script_name, sql_to_run, sql_erroneous_part, error_message, repository_version, repository_path);
            }
        }

        private string create_hash(string sql_to_run)
        {
            return crypto_provider.hash(sql_to_run.Replace(@"'", @"''"));
        }

        public bool this_is_an_every_time_script(string script_name, bool run_this_script_every_time)
        {
            var this_is_an_everytime_script = run_this_script_every_time;

            if (script_name.to_lower().StartsWith("everytime."))
            {
                this_is_an_everytime_script = true;
            }

            if (script_name.to_lower().Contains(".everytime."))
            {
                this_is_an_everytime_script = true;
            }

            return this_is_an_everytime_script;
        }

        private bool this_script_has_run_already(string script_name)
        {
            return database.has_run_script_already(script_name);
        }

        private bool this_is_a_one_time_script_that_has_changes_but_has_already_been_run(string script_name, string sql_to_run, bool run_this_script_once)
        {
            return this_script_has_changed_since_last_run(script_name, sql_to_run) && this_script_has_run_already(script_name) && run_this_script_once;
        }

        private bool this_script_has_changed_since_last_run(string script_name, string sql_to_run)
        {
            string old_text_hash = string.Empty;
            try
            {
                old_text_hash = database.get_current_script_hash(script_name);
            }
            catch (Exception ex)
            {
                log_warning_event_on_bound_logger("{0} - I didn't find this script executed before. {1}{2}Stack Trace:{2}{3}", 
                    script_name, 
                    ex.Message,
                    System.Environment.NewLine,
                    ex.StackTrace);
            }

            if (string.IsNullOrEmpty(old_text_hash)) return true;

            string new_text_hash = create_hash(sql_to_run);

            bool hash_is_same = hashes_are_equal(new_text_hash, old_text_hash);

            if (!hash_is_same)
            {
                // extra checks if only line endings have changed
                hash_is_same = have_same_hash_ignoring_platform(sql_to_run, old_text_hash);
                if (hash_is_same)
                {
                    log_warning_event_on_bound_logger("Script {0} had different line endings than before but equal content", script_name);
                }
            }

            return !hash_is_same;
        }

        private bool hashes_are_equal(string new_text_hash, string old_text_hash)
        {
            return string.Compare(old_text_hash, new_text_hash, true) == 0;
        }

        private bool have_same_hash_ignoring_platform(string sql_to_run, string old_text_hash)
        {
            // check with unix and windows line endings
            const string line_ending_windows = "\r\n";
            const string line_ending_unix = "\n";
            string new_text_hash = create_hash(sql_to_run.Replace(line_ending_windows, line_ending_unix));
            bool hash_is_same = hashes_are_equal(new_text_hash, old_text_hash);

            if (!hash_is_same)
            {
                // try other way around
                new_text_hash = create_hash(sql_to_run.Replace(line_ending_unix, line_ending_windows));
                hash_is_same = hashes_are_equal(new_text_hash, old_text_hash);
            }

            return hash_is_same;
        }

        private bool this_script_should_run(string script_name, string sql_to_run, bool run_this_script_once, bool run_this_script_every_time)
        {
            if (this_is_an_every_time_script(script_name, run_this_script_every_time))
            {
                return true;
            }

            if (is_running_all_any_time_scripts && !run_this_script_once)
            {
                return true;
            }

            if (this_script_has_run_already(script_name)
                && !this_script_has_changed_since_last_run(script_name, sql_to_run))
            {
                return false;
            }
            
            return true;
        }

        public bool this_is_an_environment_file_and_its_in_the_right_environment(string script_name, Environment environment)
        {
            log_debug_event_on_bound_logger("Checking to see if {0} is an environment file. We are in the {1} environment.", script_name, environment.name);
            if (this_is_not_an_environment_file(script_name))
            {
                return false;
            }

            bool environment_file_is_in_the_right_environment = script_name.to_lower().StartsWith(environment.name.to_lower() + ".");

            if (script_name.to_lower().Contains("." + environment.name.to_lower() + "."))
            {
                environment_file_is_in_the_right_environment = true;
            }

            if (configuration.DryRun)
            {
                log_info_event_on_bound_logger(
                    " {3} {0} is an environment file. We are in the {1} environment. This would{2} have run.",
                    script_name,
                    environment.name,
                    environment_file_is_in_the_right_environment ? string.Empty : " NOT",
                    environment_file_is_in_the_right_environment ? "->" : "  ");
            }
            else
            {
                log_info_event_on_bound_logger(
                    " {3} {0} is an environment file. We are in the {1} environment. This will{2} run.",
                    script_name,
                    environment.name,
                    environment_file_is_in_the_right_environment ? string.Empty : " NOT",
                    environment_file_is_in_the_right_environment ? "->" : "  ");
            }

            return environment_file_is_in_the_right_environment;
        }

        public static bool this_is_not_an_environment_file(string script_name)
        {
            return !script_name.to_lower().Contains(".env.");
        }
    }
}