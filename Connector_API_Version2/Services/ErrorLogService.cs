using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using MysqlEfCoreDemo.Data;
using MysqlEfCoreDemo.Models;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace MysqlEfCoreDemo.Services
{
    public class ErrorLogService : IErrorLogService
    {
        private readonly string _connectionString;

        public ErrorLogService(IConfiguration configuration)
        {
            _connectionString = configuration.GetConnectionString("Mysql");
        }

        public async Task LogErrorAsync(ErrorLog objerrorlog)
        {
            using (var connect = new MySqlConnection(_connectionString))
            {
                await connect.OpenAsync();

                using (var command = connect.CreateCommand())
                {
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = "pr_con_ins_errorlog";

                    command.Parameters.AddWithValue("in_errorlog_pipeline_code", objerrorlog.in_errorlog_pipeline_code);
                    command.Parameters.AddWithValue("in_errorlog_scheduler_gid", objerrorlog.in_errorlog_scheduler_gid);
                    command.Parameters.AddWithValue("in_errorlog_type", objerrorlog.in_errorlog_type);
                    command.Parameters.AddWithValue("in_errorlog_exception", objerrorlog.in_errorlog_exception);
                    command.Parameters.AddWithValue("in_created_by", objerrorlog.in_created_by);

                    await command.ExecuteNonQueryAsync();
                }
            }
        }
    }

}
