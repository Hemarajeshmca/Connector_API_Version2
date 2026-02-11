using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using MysqlEfCoreDemo.Data;
using Newtonsoft.Json;
using System;
using System.Data;
using System.Linq;
using static MysqlEfCoreDemo.Data.UserInfoModel;

namespace MysqlEfCoreDemo.Controllers
{
    public class ReportController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        private readonly MyDbContext dbContext;
        ReportData objDS = new ReportData();
        string targetconnectionString = "";


        public ReportController(MyDbContext dbContext, IConfiguration configuration)
        {
            _configuration = configuration;
            targetconnectionString = _configuration.GetConnectionString("targetMysql");
            this.dbContext = dbContext;
        }

        //[HttpGet("PPLDSmappedReport")]
        public IActionResult PPLDSmappedReport(string pipeline_code, string dataset_code)
        {
            headerValue header_value = new headerValue();
            DataTable response = new DataTable();
            try
            {
                var getvalue = Request.Headers.TryGetValue("user_code", out var user_code) ? user_code.First() : "";
                var getlangCode = Request.Headers.TryGetValue("lang_code", out var lang_code) ? lang_code.First() : "";
                var getRoleCode = Request.Headers.TryGetValue("role_code", out var role_code) ? role_code.First() : "";
                header_value.user_code = getvalue;
                header_value.lang_code = getlangCode;
                header_value.role_code = getRoleCode;
                response = objDS.PPLDSmappedData(pipeline_code, dataset_code, header_value, targetconnectionString);
                var serializedProduct = JsonConvert.SerializeObject(response, Formatting.None);
                return Ok(serializedProduct);
            }
            catch (Exception e)
            {
                return Problem(title: e.Message);
            }
        }

        //[HttpGet("FileimportReport")]
        public IActionResult FileimportReport(string from_date, string to_date)
        {
            headerValue header_value = new headerValue();
            DataTable response = new DataTable();
            try
            {
                var getvalue = Request.Headers.TryGetValue("user_code", out var user_code) ? user_code.First() : "";
                var getlangCode = Request.Headers.TryGetValue("lang_code", out var lang_code) ? lang_code.First() : "";
                var getRoleCode = Request.Headers.TryGetValue("role_code", out var role_code) ? role_code.First() : "";
                header_value.user_code = getvalue;
                header_value.lang_code = getlangCode;
                header_value.role_code = getRoleCode;
                response = objDS.FileimportData(from_date, to_date, header_value, targetconnectionString);
                var serializedProduct = JsonConvert.SerializeObject(response, Formatting.None);
                return Ok(serializedProduct);
            }
            catch (Exception e)
            {
                return Problem(title: e.Message);
            }
        }
    }
}
