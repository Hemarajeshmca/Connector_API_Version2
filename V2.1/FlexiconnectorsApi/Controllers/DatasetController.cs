using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using static MysqlEfCoreDemo.Data.UserInfoModel;
using static MysqlEfCoreDemo.Models.DatasetModel;
using System.Linq;
using System;
using System.Data;
using MysqlEfCoreDemo.Data;


namespace MysqlEfCoreDemo.Controllers
{
    [Route("[controller]")]
    public class DatasetController : Controller
    {
        private IConfiguration _configuration;
        DatasetData objDS = new DatasetData();
        public DatasetController(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        string constring = "";

        #region DataSet DML
        [HttpGet("GetDataSet")]
        public IActionResult GetDataSet(string pipeline_code, string source_fields, string datasetType)
        {
            constring = _configuration.GetSection("ConnectionStrings")["Mysql"].ToString();
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
                response = objDS.GetDatasetdata(pipeline_code, header_value, constring, source_fields, datasetType);
                var serializedProduct = JsonConvert.SerializeObject(response, Formatting.None);
                return Ok(serializedProduct);
            }
            catch (Exception e)
            {
                return Problem(title: e.Message);
            }
        }

        [HttpGet("GetDataSetField")]
        public IActionResult GetDataSetField(string pipeline_code, string datset_code)
        {
            constring = _configuration.GetSection("ConnectionStrings")["Mysql"].ToString();
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
                response = objDS.GetDatasetFielddata(pipeline_code, datset_code, header_value, constring);
                var serializedProduct = JsonConvert.SerializeObject(response, Formatting.None);
                return Ok(serializedProduct);
            }
            catch (Exception e)
            {
                return Problem(title: e.Message);
            }
        }
        
        #endregion

        [HttpPost("DatasetHeader")]
        public IActionResult DatasetHeader([FromBody] DatasetHeadermodel Datasetheadermodel)
        {
            constring = _configuration.GetSection("ConnectionStrings")["Mysql"].ToString();
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
                response = objDS.DatasetHeaderdata(Datasetheadermodel, header_value, constring);
                var serializedProduct = JsonConvert.SerializeObject(response, Formatting.None);
                return Ok(serializedProduct);
            }
            catch (Exception e)
            {
                return Problem(title: e.Message);
            }
        }

        [HttpPost("DatasetDetail")]
        public IActionResult DatasetDetail([FromBody] Datasetdetailmodel Datasetdetailmodel)
        {
            constring = _configuration.GetSection("ConnectionStrings")["Mysql"].ToString();
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
                response = objDS.DatasetDetaildata(Datasetdetailmodel, header_value, constring);
                var serializedProduct = JsonConvert.SerializeObject(response, Formatting.None);
                return Ok(serializedProduct);
            }
            catch (Exception e)
            {
                return Problem(title: e.Message);
            }
        }

        [HttpPost("ClonePipelineDataset")]
        public IActionResult ClonePipelineDataset([FromBody] ClonePipelineDatasetModel objClonePipelineDatasetModel)
        {
            constring = _configuration.GetSection("ConnectionStrings")["Mysql"].ToString();
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
                response = objDS.ClonePipelineDatasetData(objClonePipelineDatasetModel, header_value, constring);
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
