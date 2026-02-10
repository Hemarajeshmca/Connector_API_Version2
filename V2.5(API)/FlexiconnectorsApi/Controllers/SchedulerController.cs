using Cake.Core.IO;
using log4net.Util;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Graph.Models.Security;
using MySql.Data.MySqlClient;
using MysqlEfCoreDemo.Data;
using MysqlEfCoreDemo.Models;
using MysqlEfCoreDemo.Services;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NPOI.OpenXmlFormats.Wordprocessing;
using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using DataSet = System.Data.DataSet;
using Path = System.IO.Path;

namespace MysqlEfCoreDemo.Controllers
{
    public class SchedulerController : ControllerBase
    {

        #region Global variables
        string conn = "";
        string errorlogfilePath = ""; //"D:\\Mohan\\error_log.txt";
        string errormsg = "";
        string hostingfor = "";
        string _slash = "";
        string msg = "";
        int out_result = 0;
        string constring = "";

        string csvfilePath = "";
        string lineterm = "\r\n";
        string targetconnectionString = "";
        List<JObject> extractedList = new List<JObject>();
        int sched_gid = 0;
        #endregion

        private readonly IConfiguration _configuration;
        private readonly IErrorLogService _errorlog;
        private readonly MyDbContext dbContext;

        public SchedulerController(MyDbContext dbContext, IConfiguration configuration, IErrorLogService errorLogService)
        {
            _configuration = configuration;
            _errorlog = errorLogService;
            hostingfor = _configuration["HostingFor"];// _configuration.GetConnectionString("HostingFor");
            if (hostingfor.Trim() == "Linux")
            {
                _slash = "/";
            }
            else
            {
                _slash = "\\";
            }
            conn = _configuration["conn"];
            errorlogfilePath = System.IO.Path.Combine(Directory.GetCurrentDirectory(), "Errorlog", "error_log.txt");
            targetconnectionString = _configuration.GetConnectionString("targetMysql");
            csvfilePath = System.IO.Path.Combine(Directory.GetCurrentDirectory(), "Processing") + _slash;
            this.dbContext = dbContext;

        }

        #region Windows Releavent Api
        [HttpGet]
        public async Task<IActionResult> GetSchedulerPath(string pipeline_code, string dataset_code)
        {
            try
            {
                // Check if pipeline exists in the main table
                var pipelineExists = await dbContext.con_mst_tpipeline
                    .AnyAsync(p => p.pipeline_code == pipeline_code && p.pipeline_status == "Active" && p.delete_flag == "N");

                if (!pipelineExists)
                    return NotFound("This pipeline is not Active..!");

                // Get scheduler_path from the pipeline detail table
                var pipelineDetail = await (from d in dbContext.con_trn_tpipelinedetails
                                            join f in dbContext.con_trn_tpplfinalization
                                            on new { d.pipeline_code, dataset_code = d.target_dataset_code }
                                            equals new { f.pipeline_code, dataset_code = f.dataset_code }
                                            where d.pipeline_code == pipeline_code
                                                  && d.target_dataset_code == dataset_code
                                                  && d.pipelinedet_status == "Active"
                                                  && d.delete_flag == "N"
                                                  && f.delete_flag == "N"
                                            select new
                                            {
                                                d.scheduler_path,
                                                f.run_type
                                            }
                                        ).FirstOrDefaultAsync();
                if (pipelineDetail == null)
                    return NotFound("Scheduler path not found ..!");

                return Ok(pipelineDetail);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> UploadSchedulerFile_validation(string file_path, string pipeline_code, string dataset_code, IFormFile inputfile)
        {

            if (inputfile == null || inputfile.Length == 0)
                return BadRequest("No file uploaded.");

            string file_name = Path.GetFileName(inputfile.FileName);
            string fileExtension = Path.GetExtension(file_name).ToLower();

            // Normalize and build full folder path (Linux safe)
            file_path = file_path.Replace("\\", "/");
            string folder_path = Path.GetDirectoryName(file_path);

            try
            {

                // 0️.0 Check if folder exists
                if (!Directory.Exists(folder_path))
                {
                    return BadRequest($"{folder_path} — The specified folder does not exist or is inaccessible.");
                }
                //Getting Src Column from  con_trn_tpplsourcefield
                var sourcecolumns = (from a in dbContext.con_trn_tpplsourcefield
                                     where a.pipeline_code == pipeline_code
                                     where a.dataset_code == dataset_code
                                     where a.source_type != "Expression"
                                     where a.delete_flag == "N"
                                     orderby a.sourcefield_sno
                                     select new
                                     {
                                         a.sourcefield_name,
                                         a.sourcefield_sno
                                     }).ToList();

                // GetSheet name
                string excel_sheetName = dbContext.con_mst_tpipeline
                    .Where(p => p.pipeline_code == pipeline_code && p.pipeline_status == "Active" && p.delete_flag == "N")
                    .Select(a => a.sheet_name)
                    .FirstOrDefault();


                // 0.1) File Header Validation
                if (fileExtension == ".xlsx")
                {
                    using (var stream = inputfile.OpenReadStream())
                    using (var workbook = new ClosedXML.Excel.XLWorkbook(stream))
                    {
                        // Check if sheet exists
                        var worksheet = workbook.Worksheets.FirstOrDefault(ws => ws.Name.Equals(excel_sheetName, StringComparison.OrdinalIgnoreCase));
                        if (worksheet == null)
                            throw new Exception($"Sheet name mismatch! Expected '{excel_sheetName}'.");

                        // Validate headers
                        foreach (var items in sourcecolumns)
                        {
                            var actual = worksheet.Cell(1, items.sourcefield_sno).GetValue<string>().Trim().ToLower();
                            var expected = items.sourcefield_name.Trim().ToLower();

                            if (actual != expected)
                                throw new Exception("File Header Mismatch!");
                        }
                    }
                }
                else if (fileExtension == ".xls")
                {
                    using (var stream = inputfile.OpenReadStream())
                    using (var workbook = new NPOI.HSSF.UserModel.HSSFWorkbook(stream))
                    {
                        bool sheetFound = false;
                        for (int i = 0; i < workbook.NumberOfSheets; i++)
                        {
                            if (workbook.GetSheetName(i).Equals(excel_sheetName, StringComparison.OrdinalIgnoreCase))
                            {
                                sheetFound = true;
                                break;
                            }
                        }

                        if (!sheetFound)
                            throw new Exception($"Sheet name mismatch! Expected '{excel_sheetName}'.");

                        // If found, get that sheet
                        var sheet = workbook.GetSheet(excel_sheetName);
                        var headerRow = sheet.GetRow(0);

                        foreach (var items in sourcecolumns)
                        {
                            var cell = headerRow.GetCell(items.sourcefield_sno - 1);
                            var actual = cell?.ToString().Trim().ToLower() ?? "";
                            var expected = items.sourcefield_name.Trim().ToLower();

                            if (actual != expected)
                                throw new Exception("File Header Mismatch!");
                        }
                    }
                }
                else
                {
                    return BadRequest("Unsupported file format. Only .xlsx and .xls are supported.");
                }

                // 0.2 Check if file already exists in folder
                if (System.IO.File.Exists(file_path))
                {
                    return BadRequest("File already exists in the specified folder.");
                }

                // 1️) Validate pipeline exists
                var pipelineExists = await dbContext.con_mst_tpipeline
                    .AnyAsync(p => p.pipeline_code == pipeline_code && p.pipeline_status == "Active" && p.delete_flag == "N");

                if (!pipelineExists)
                    return NotFound("This pipeline is not Active..!");

                // 2️) Check Fieldmapping
                DataTable dataTable = await FieldmappingDT(pipeline_code, dataset_code);

                if (dataTable.Rows.Count <= 0)
                    return BadRequest("Fieldmapping is not done for this pipeline...");

                // 3️) Duplicate File Name Validation
                var DuplicateFilename = await dbContext.con_trn_tscheduler
                        .Where(a => a.pipeline_code == pipeline_code
                        && (a.scheduler_status == "Completed" || a.scheduler_status == "Ratified")
                        && a.file_name == file_name
                        && a.delete_flag == "N")
                        .CountAsync();

                if (DuplicateFilename > 0)
                {
                    return BadRequest("Duplicate File Name.");
                }

                return Ok("Validation passed successfully.");
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<DataTable> FieldmappingDT(string pipelinecode, string datasetcode = "")
        {
            DataTable dataTable = new DataTable();

            var ds_code = await dbContext.con_trn_tpplsourcefield
            .Where(a => a.pipeline_code == pipelinecode
            && a.dataset_code == datasetcode
            && a.source_type != "Expression"
            && a.delete_flag == "N")
            .Select(a => new
            {
                //dataset_field_name = a.dataset_field_name,
                ppl_field_name = a.sourcefield_name
            }).ToListAsync();

            // Define the columns in the DataTable
            dataTable.Columns.Add("ppl_field_name");
            dataTable.Columns.Add("default_value");

            // Populate the DataTable with data from the query
            foreach (var item in ds_code)
            {
                DataRow row = dataTable.NewRow();
                row["ppl_field_name"] = item.ppl_field_name;
                //row["default_value"] = item.default_value;
                dataTable.Rows.Add(row);
            }
            return dataTable;
        }
        #endregion

        #region Source Api Scheduled Proccessing

        [HttpPost]
        public async Task<IActionResult> ApiScheduledProcess_Taskscheduler([FromBody] NewSchedulerForothers objsched)
        {
            string msg = "";
            sched_gid = objsched.scheduler_gid;
            try
            {

                var count = await dbContext.con_mst_tpipeline
                        .Where(a => a.pipeline_code == objsched.pipeline_code && a.delete_flag == "N" && a.pipeline_status == "Active")
                        .CountAsync();
                var getsch = dbContext.con_trn_tscheduler
                        .Where(p => p.scheduler_gid == objsched.scheduler_gid
                        && p.pipeline_code == objsched.pipeline_code
                        && p.dataset_code == objsched.dataset_code
                        && p.scheduler_status == "Locked" && p.delete_flag == "N")
                        .Select(p => new Scheduler
                        {
                            scheduler_status = p.scheduler_status,
                            file_path = p.file_path,
                            file_name = p.file_name
                        })
                        .SingleOrDefault();
                if (count > 0)
                {
                    if (getsch != null)
                    {
                        var pipelineWithConnector = await dbContext.con_mst_tpipeline
                       .Where(p => p.pipeline_code == objsched.pipeline_code && p.delete_flag == "N")
                       .Join(
                           dbContext.con_mst_tconnection,
                                       pipeline => pipeline.connection_code,
                           connector => connector.connection_code,
                           (pipeline, connector) => new { Pipeline = pipeline, Connector = connector }
                       )
                       .FirstOrDefaultAsync();
                        if (pipelineWithConnector.Connector.source_db_type == "API")
                        {
                            string dataset_code = "";
                            string query1 = "";
                            bool mdf_flag = false;
                            // 1. Fetch API details from DB
                            var apiDetails = dbContext.con_trn_tpplapiheader
                                .Where(x => x.pipeline_code == objsched.pipeline_code && x.delete_flag == "N")
                                .FirstOrDefault();
                            if (apiDetails == null)
                                throw new Exception("API configuration not found.");
                            string apiUrl = apiDetails.api_url;
                            string apiMethod = apiDetails.api_method?.ToUpper() ?? "GET";
                            string apiPayload = apiDetails.api_payload ?? "";
                            string sampleJsonResponse = apiDetails.json_response ?? "";
                            string jsonResponse = "";
                            string finalToken = "";
                            // Deserialize header dictionary
                            var headersDict = JsonConvert.DeserializeObject<Dictionary<string, string>>(apiDetails.api_header);
                            // 2. Auth token API call
                            if (apiDetails.have_auth_url == "Y")
                            {
                                using (HttpClient authClient = new HttpClient())
                                {
                                    var authPayload = new
                                    {
                                        username = apiDetails.auth_user_name,
                                        password = apiDetails.auth_user_pswd
                                    };
                                    StringContent authContent = new StringContent(
                                        JsonConvert.SerializeObject(authPayload),
                                        Encoding.UTF8,
                                        "application/json"
                                    );
                                    HttpResponseMessage authResp = authClient.PostAsync(apiDetails.auth_url, authContent).Result;
                                    authResp.EnsureSuccessStatusCode();
                                    string authResult = authResp.Content.ReadAsStringAsync().Result;
                                    JObject authObj = JObject.Parse(authResult);
                                    finalToken = authObj[apiDetails.auth_token_keyname]?.ToString();
                                }
                            }
                            // 3. Main API call
                            // Prepare HttpClient
                            using (HttpClient client = new HttpClient())
                            {
                                // Add all headers except Content-Type
                                if (headersDict != null)
                                {
                                    foreach (var h in headersDict)
                                    {
                                        if (h.Key.Equals("Content-Type", StringComparison.OrdinalIgnoreCase))
                                            continue; // skip, handle later
                                        client.DefaultRequestHeaders.TryAddWithoutValidation(h.Key, h.Value);
                                    }
                                }
                                HttpContent content = null;
                                // Create content for POST or PUT
                                if (apiMethod == "POST" || apiMethod == "PUT")
                                {
                                    string contentType = "application/json";
                                    if (headersDict != null && headersDict.ContainsKey("Content-Type"))
                                    {
                                        contentType = headersDict["Content-Type"];
                                    }
                                    content = new StringContent(apiPayload, Encoding.UTF8, contentType);
                                }
                                HttpResponseMessage response = null;
                                switch (apiMethod)
                                {
                                    case "POST":
                                        response = client.PostAsync(apiUrl, content).Result;
                                        break;
                                    case "PUT":
                                        response = client.PutAsync(apiUrl, content).Result;
                                        break;
                                    case "DELETE":
                                        response = client.DeleteAsync(apiUrl).Result;
                                        break;
                                    default: // GET
                                        response = client.GetAsync(apiUrl).Result;
                                        break;
                                }
                                response.EnsureSuccessStatusCode();
                                jsonResponse = response.Content.ReadAsStringAsync().Result;
                            }
                            // 4. Convert JSON into JToken
                            var token = JToken.Parse(jsonResponse);
                            //Get All scheduled list against piplinecode
                            //var apischeduledlist = dbContext.con_trn_tscheduler
                            //    .Where(x => x.pipeline_code == objsched.pipeline_code
                            //    && (x.scheduler_status == "Scheduled" || x.scheduler_status == "Locked")
                            //    && x.delete_flag == "N")
                            //    .ToList();

                            var apischeduledlist = dbContext.con_trn_tpipelinedetails
                                .Where(x => x.pipeline_code == objsched.pipeline_code
                                && (x.pipelinedet_status == "Active")
                                && x.delete_flag == "N")
                                .ToList();

                            // Validate Json Global variables and default values
                            OkObjectResult result;
                            dynamic validatejsonRes = new ExpandoObject();
                            validatejsonRes.valid = true;

                            //Loop aginst piplinecode and datasetcode
                            for (int f = 0; f < apischeduledlist.Count; f++)
                            {
                                DataSet ds = new DataSet();
                                extractedList.Clear();
                                query1 = "";
                                dataset_code = apischeduledlist[f].target_dataset_code.ToString();
                                //sched_gid = apischeduledlist[f].scheduler_gid;

                                //STEP 0.1: validate sampleJSON structure is same or not
                                if (validatejsonRes.valid == true && sampleJsonResponse != "[]" && sampleJsonResponse != "")
                                {
                                    JsonInput objjsoninput = new JsonInput();
                                    objjsoninput.DataJson = jsonResponse;
                                    objjsoninput.TemplateJson = sampleJsonResponse;
                                    result = ValidateJson(objjsoninput) as OkObjectResult;
                                    validatejsonRes = result.Value;
                                }

                                if (validatejsonRes.valid == true)
                                {
                                    //STEP 1: Call the FieldmappingDT method
                                    DataTable dataTable = FieldmappingDT(objsched.pipeline_code, dataset_code).Result;
                                    if (dataTable.Rows.Count <= 0)
                                    {
                                        // logger("After FieldmappingDT");
                                        await UpdateScheduler(sched_gid, "Failed", "System");
                                        msg = "Fieldmapping is not done for this pipeline...";
                                        var errorLog = new ErrorLog
                                        {
                                            in_errorlog_pipeline_code = objsched.pipeline_code,
                                            in_errorlog_scheduler_gid = sched_gid,
                                            in_errorlog_type = "Catch - Method Name : FieldmappingDT",
                                            in_errorlog_exception = msg,
                                            in_created_by = objsched.initiated_by
                                        };
                                        _errorlog.LogErrorAsync(errorLog);
                                    }
                                    bool dtColumnsCreated = false;
                                    // 5. Navigate to result set
                                    Jsonreader(token.ToString(), objsched.pipeline_code, dataset_code);
                                    DataTable dtExtract = ConvertToDataTable(extractedList);

                                    // API Header Filter
                                    var apiheaderFilter = dbContext.con_trn_tpplapiheader
                                        .FirstOrDefault(x => x.pipeline_code == objsched.pipeline_code
                                                          && x.delete_flag == "N");

                                    DataTable dtFiltered = dtExtract.Copy(); // Start with full data

                                    // ---------- Inclusion Filter ----------
                                    if (!string.IsNullOrWhiteSpace(apiheaderFilter.inclusion_filter_cond))
                                    {
                                        string filter = apiheaderFilter.inclusion_filter_cond.Replace("!=", "<>");
                                        DataRow[] rows = null;

                                        try
                                        {
                                            rows = dtFiltered.Select(filter);
                                        }
                                        catch
                                        {
                                            rows = Array.Empty<DataRow>();
                                        }

                                        dtFiltered = rows.Length > 0 ? rows.CopyToDataTable() : dtFiltered.Clone();
                                    }

                                    // ---------- Rejection Filter ----------
                                    if (!string.IsNullOrWhiteSpace(apiheaderFilter.rejection_filter_cond))
                                    {
                                        string cond = apiheaderFilter.rejection_filter_cond.Trim();

                                        cond = cond.Replace("!=", "<>");

                                        string reversed = cond;

                                        if (cond.Contains(">="))
                                            reversed = cond.Replace(">=", "<");
                                        else if (cond.Contains("<="))
                                            reversed = cond.Replace("<=", ">");
                                        else if (cond.Contains("<>"))
                                            reversed = cond.Replace("<>", "=");
                                        else if (cond.Contains(">"))
                                            reversed = cond.Replace(">", "<=");
                                        else if (cond.Contains("<"))
                                            reversed = cond.Replace("<", ">=");
                                        else if (cond.Contains("="))
                                            reversed = cond.Replace("=", "<>");

                                        DataRow[] rows = null;

                                        try
                                        {
                                            rows = dtFiltered.Select(reversed);
                                        }
                                        catch
                                        {
                                            rows = Array.Empty<DataRow>();
                                        }

                                        dtFiltered = rows.Length > 0 ? rows.CopyToDataTable() : dtFiltered.Clone();
                                    }


                                    // Get mapped columns from database
                                    var bcpcolumns = (from a in dbContext.con_trn_tpplsourcefield
                                                      where a.pipeline_code == objsched.pipeline_code
                                                      where a.dataset_code == dataset_code
                                                      where a.sourcefieldmapping_flag == "Y"
                                                      where a.delete_flag == "N"
                                                      orderby a.dataset_table_field_sno
                                                      select new
                                                      {
                                                          a.sourcefield_sno,
                                                          a.sourcefield_name,
                                                          a.sourcefield_datatype,
                                                          a.dataset_table_field,
                                                          a.source_type
                                                      }).ToList();
                                    // Get source columns
                                    var sourcecolumns = (from a in dbContext.con_trn_tpplsourcefield
                                                         where a.pipeline_code == objsched.pipeline_code
                                                         where a.dataset_code == dataset_code
                                                         where a.source_type != "Expression"
                                                         where a.delete_flag == "N"
                                                         orderby a.sourcefield_sno
                                                         select new
                                                         {
                                                             a.sourcefield_name,
                                                             a.sourcefield_sno
                                                         }).ToList();
                                    // Inclusion condition Apply
                                    var filtercond = dbContext.con_trn_tpplcondition
                                                     .Where(p => p.pipeline_code == objsched.pipeline_code
                                                                 && p.dataset_code == dataset_code
                                                                 && p.condition_type == "Filter"
                                                                 && p.delete_flag == "N")
                                                     .Select(a => new
                                                     {
                                                         condition_text = a.condition_text
                                                     }).ToList();
                                    if (filtercond.Any() && !string.IsNullOrEmpty(filtercond[0].condition_text))
                                    {
                                        query1 = " and (" + filtercond[0].condition_text + ")";
                                    }
                                    // Exclusion condition Apply
                                    var rejectioncond = dbContext.con_trn_tpplcondition
                                                       .Where(p => p.pipeline_code == objsched.pipeline_code
                                                                   && p.dataset_code == dataset_code
                                                                   && p.condition_type == "Rejection"
                                                                   && p.delete_flag == "N")
                                                       .Select(a => new
                                                       {
                                                           condition_text = a.condition_text
                                                       }).ToList();
                                    if (rejectioncond.Any() && !string.IsNullOrEmpty(rejectioncond[0].condition_text) && !mdf_flag)
                                    {
                                        string modifiedCondition = rejectioncond[0].condition_text;
                                        if (rejectioncond[0].condition_text.Contains("="))
                                        {
                                            modifiedCondition = rejectioncond[0].condition_text.Replace("=", "<>");
                                        }
                                        else if (rejectioncond[0].condition_text.Contains(">"))
                                        {
                                            modifiedCondition = rejectioncond[0].condition_text.Replace(">", "<");
                                        }
                                        else if (rejectioncond[0].condition_text.Contains("<"))
                                        {
                                            modifiedCondition = rejectioncond[0].condition_text.Replace("<", ">");
                                        }
                                        if (!string.IsNullOrEmpty(modifiedCondition))
                                        {
                                            query1 += " and (" + modifiedCondition + ")";
                                            mdf_flag = true;
                                        }
                                    }
                                    // 6. Build DataTable structure with correct datatypes
                                    DataTable dt = new DataTable();
                                    string[,] colType = new string[bcpcolumns.Count, 3];
                                    int idx = 0;
                                    foreach (var items in bcpcolumns)
                                    {
                                        Type columnType = typeof(string);
                                        switch (items.sourcefield_datatype.ToUpper())
                                        {
                                            case "NUMERIC":
                                                columnType = items.source_type.ToUpper() == "EXPRESSION" ? typeof(string) : typeof(double);
                                                break;
                                            case "INTEGER":
                                                columnType = items.source_type.ToUpper() == "EXPRESSION" ? typeof(string) : typeof(int);
                                                break;
                                            default:
                                                columnType = typeof(string);
                                                break;
                                        }
                                        dt.Columns.Add(items.sourcefield_name, columnType);
                                        colType[idx, 0] = items.sourcefield_name;
                                        colType[idx, 1] = items.sourcefield_sno.ToString();
                                        colType[idx, 2] = items.sourcefield_datatype.ToUpper();
                                        idx++;
                                    }
                                    //7.Populate final table with correct types
                                    foreach (DataRow row in dtFiltered.Rows)
                                    {
                                        DataRow newRow = dt.NewRow();
                                        for (idx = 0; idx < bcpcolumns.Count; idx++)
                                        {
                                            string colName = colType[idx, 0];
                                            string dtype = colType[idx, 2];
                                            if (!dtFiltered.Columns.Contains(colName))
                                                continue;
                                            string value = row[colName]?.ToString();
                                            switch (dtype)
                                            {
                                                case "NUMERIC":
                                                    newRow[colName] = double.TryParse(value, out var d) ? d : DBNull.Value;
                                                    break;
                                                case "INTEGER":
                                                    newRow[colName] = int.TryParse(value, out var iVal) ? iVal : DBNull.Value;
                                                    break;
                                                default:
                                                    newRow[colName] = value ?? "";
                                                    break;
                                            }
                                        }
                                        dt.Rows.Add(newRow);
                                    }
                                    // 8. Filtering
                                    query1 = query1.Replace("!=", "<>");
                                    string filterExpression = "1=1 " + query1;

                                    DataTable filteredDt = dt.Select(filterExpression).Any()
                                        ? dt.Select(filterExpression).CopyToDataTable()
                                        : dt.Clone();
                                    // Final output
                                    filteredDt.TableName = "Result";
                                    ds.Tables.Add(filteredDt);
                                    msg = await DatatableToCSV(ds.Tables[0], objsched.pipeline_code, dataset_code);
                                }
                                else
                                {
                                    await UpdateScheduler(objsched.scheduler_gid, "Failed", "System");
                                    string errorJson = JsonConvert.SerializeObject(validatejsonRes.errors);
                                    var errorLog = new ErrorLog
                                    {
                                        in_errorlog_pipeline_code = objsched.pipeline_code,
                                        in_errorlog_scheduler_gid = sched_gid,
                                        in_errorlog_type = "Catch - Method Name : ApiScheduledProcess_Taskscheduler",
                                        in_errorlog_exception = errorJson,
                                        in_created_by = objsched.initiated_by
                                    };
                                    _errorlog.LogErrorAsync(errorLog);
                                    await Reschedulefornexttime_api(objsched.pipeline_code);
                                }
                            }
                            await UpdateScheduler(objsched.scheduler_gid, "Completed", "System");
                            await Reschedulefornexttime_api(objsched.pipeline_code);

                        }
                        else
                        {
                            msg = "This Pipeline is already in <" + getsch.scheduler_status + "> status";
                        }
                    }
                    else
                    {
                        msg = "This is not a Active pipeline";
                    }
                }
            }
            catch (Exception ex)
            {
                await UpdateScheduler(objsched.scheduler_gid, "Failed", "System");
                var errorLog = new ErrorLog
                {
                    in_errorlog_pipeline_code = objsched.pipeline_code,
                    in_errorlog_scheduler_gid = sched_gid,
                    in_errorlog_type = "Catch - Method Name : ApiScheduledProcess_Taskscheduler",
                    in_errorlog_exception = ex.Message,
                    in_created_by = objsched.initiated_by
                };
                _errorlog.LogErrorAsync(errorLog);
                await Reschedulefornexttime_api(objsched.pipeline_code);
                msg = ex.Message;
            }
            return Ok(msg);
        }

        private List<Node> LoadNodesFromDb(string pipeline_code, string dataset_code)
        {
            var nodeList = dbContext.con_trn_tpplapinode
                .Where(a => a.pipeline_code == pipeline_code &&
                            a.dataset_code == dataset_code &&
                            a.delete_flag == "N")
                .OrderBy(a => a.level)
                .Select(a => new Node(a.node ?? "", a.siblings ?? "", a.parent_node ?? "", a.child_node ?? ""))
                .ToList();

            return nodeList;
        }

        private void Jsonreader(string token, string pipeline_code, string dataset_code)
        {
            var txtExtractJson = "";

            JObject jsonObj = JObject.Parse(token);


            // Get nodeList from DB
            List<Node> nodeList = LoadNodesFromDb(pipeline_code, dataset_code);

            // Call original recursive function
            string json = token;
            string parent_json = "";
            int node_index = 0;

            if (nodeList.Count > 0)
            {
                jsonread(json, parent_json, nodeList, node_index);
            }
        }

        private void jsonread(string _json, string _parent_json, List<Node> _node_list, int _node_index)
        {
            string _json1 = "";

            _json = convertlist(_json);

            List<object> deserializedList = System.Text.Json.JsonSerializer.Deserialize<List<object>>(_json);

            foreach (object obj in deserializedList)
            {
                JObject parent_node1 = new JObject();

                if (!string.IsNullOrEmpty(_parent_json))
                {
                    parent_node1 = JObject.Parse(_parent_json);
                }

                var dict = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, object>>(obj.ToString());

                //
                // HANDLE SIBLINGS
                //
                if (!string.IsNullOrEmpty(_node_list[_node_index].siblingNode))
                {
                    foreach (var kv in dict)
                    {
                        if (_node_list[_node_index].siblingNode.Split(',').Contains(kv.Key))
                        {
                            JObject jsonobj = new JObject();
                            jsonobj.Add(kv.Key, kv.Value?.ToString() ?? "");

                            if (string.IsNullOrEmpty(_parent_json))
                            {
                                parent_node1 = jsonobj;
                                _parent_json = Newtonsoft.Json.JsonConvert.SerializeObject(parent_node1);
                            }
                            else
                            {
                                parent_node1.Merge(jsonobj);
                            }
                        }
                    }
                }
                else if (!string.IsNullOrEmpty(_parent_json) && string.IsNullOrEmpty(_node_list[_node_index].siblingNode))
                {
                    parent_node1 = JObject.Parse(_parent_json);
                }

                //
                // HANDLE CURRENT NODE
                //
                foreach (var kv in dict)
                {
                    if (kv.Key == _node_list[_node_index].currNode)
                    {
                        //
                        // LEAF NODE (FINAL LEVEL)
                        //
                        if (string.IsNullOrEmpty(_node_list[_node_index].nextNode))
                        {
                            string currList = convertlist(kv.Value.ToString());

                            List<object> deserializedCurrList =
                                System.Text.Json.JsonSerializer.Deserialize<List<object>>(currList);

                            foreach (object currobj in deserializedCurrList)
                            {
                                JObject currjsonobj = new JObject(parent_node1); // clone parent

                                JObject jsonobj = new JObject();

                                try
                                {
                                    jsonobj = JObject.Parse(currobj.ToString());
                                }
                                catch
                                {
                                    jsonobj.Add(kv.Key, currobj?.ToString() ?? "");
                                }

                                currjsonobj.Merge(jsonobj);

                                // ADD RESULT INTO GLOBAL LIST
                                extractedList.Add(currjsonobj);
                            }
                        }
                        else
                        {
                            //
                            // RECURSION – GO TO NEXT NODE LEVEL
                            //
                            string json1 = kv.Value?.ToString() ?? "";
                            jsonread(json1, Newtonsoft.Json.JsonConvert.SerializeObject(parent_node1), _node_list, _node_index + 1);
                        }
                    }
                }

            }
        }

        private string convertlist(string json)
        {
            string json1 = "", c = "";
            int i = 0, l = 0;

            // check first character "["
            c = json.Substring(0, 1);
            json1 = json;

            // check first character "["
            c = json1.Substring(0, 1);

            if (c != "[" && c != "{")
            {
                json1 = "\"" + json1 + "\"";
            }

            if (c != "[")
            {
                json1 = "[" + json1;
            }

            l = json1.Length;

            // check last character "]"
            c = json1.Substring(l - 1);

            if (c != "]")
            {
                json1 = json1 + "]";
            }

            return json1;
        }

        public DataTable ConvertToDataTable(List<JObject> jsonObjects)
        {
            DataTable dt = new DataTable();

            var allKeys = jsonObjects
                .SelectMany(obj => obj.Properties().Select(p => p.Name))
                .Distinct()
                .ToList();

            foreach (var key in allKeys)
                dt.Columns.Add(key, typeof(string));

            foreach (var obj in jsonObjects)
            {
                DataRow row = dt.NewRow();
                foreach (var key in allKeys)
                {
                    row[key] = obj[key]?.ToString() ?? "";
                }
                dt.Rows.Add(row);
            }

            return dt;
        }

        public async Task<string> DatatableToCSV(DataTable dt, string pipelinecode, string datasetcode = "")
        {

            string destinationTableName = "con_trn_tbcp";

            using (MySqlConnection connect = new MySqlConnection(targetconnectionString))
            {
                var pplcode = pipelinecode;
                csvfilePath = csvfilePath + sched_gid + ".csv";
                string directory = System.IO.Path.GetDirectoryName(csvfilePath);
                if (!Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }
                errormsg = "Step 7 DatatableToCSV: 859";
                System.IO.File.AppendAllText(errorlogfilePath, errormsg + Environment.NewLine);
                if (!System.IO.File.Exists(csvfilePath))
                {
                    using (FileStream fs = System.IO.File.Create(csvfilePath))
                    {

                    }
                    using (StreamWriter writer = new StreamWriter(csvfilePath))
                    {
                        foreach (DataColumn column in dt.Columns)
                        {
                            writer.Write(column.ColumnName);
                            if (column.Ordinal < dt.Columns.Count - 1)
                            {
                                writer.Write("`~*`");
                            }
                        }
                        writer.WriteLine();
                        foreach (DataRow row in dt.Rows)
                        {
                            for (int i = 0; i < dt.Columns.Count; i++)
                            {
                                writer.Write(row[i].ToString().Trim().Replace("\n", "").Replace("\r", " "));
                                if (i < dt.Columns.Count - 1)
                                {
                                    writer.Write("`~*`");
                                }
                            }
                            writer.WriteLine();
                        }
                        errormsg = "Step 8 DatatableToCSV: 890";
                        System.IO.File.AppendAllText(errorlogfilePath, errormsg + Environment.NewLine);
                        writer.Close();
                    }
                }


                if (connect.State != ConnectionState.Open)
                    connect.Open();

                var shdgid = dbContext.con_trn_tscheduler
                      .Where(p => p.scheduler_gid == sched_gid
                                  && (p.scheduler_status == "Scheduled" || p.scheduler_status == "Locked" || p.scheduler_status == "Initiated"))
                      .Select(a => new
                      {
                          scheduler_gid = a.scheduler_gid
                      })
                      .ToList();
                connect.Close();

                if (shdgid.Count > 0)
                {
                    //sched_gid = shdgid[0].scheduler_gid;
                    await UpdateScheduler(sched_gid, "Initiated", "System");

                    connect.Open();

                    var bulkLoader = new MySqlBulkLoader(connect)
                    {
                        Expressions =  {
                                    "scheduler_gid =" + sched_gid,
                               },
                        TableName = destinationTableName,
                        FieldTerminator = "`~*`",         // CSV field delimiter
                        LineTerminator = lineterm,         // CSV line terminator
                        FileName = csvfilePath,
                        NumberOfLinesToSkip = 1,      // Skip the header row if necessary
                        CharacterSet = "utf8",   // Set the character set
                        Local = true,
                        Timeout = 0
                    };

                    List<string> bcpcolumn = new List<string>();

                    for (int i = 1; dt.Columns.Count >= i; i++)
                    {
                        bcpcolumn.Add("col" + i);

                    }
                    bulkLoader.Columns.AddRange(bcpcolumn);

                    int rowsAffected = bulkLoader.Load();

                    if (System.IO.File.Exists(csvfilePath))
                    {
                        System.IO.File.Delete(csvfilePath);
                        Console.WriteLine("File deleted after processing.");
                    }

                    MySqlCommand command = connect.CreateCommand();

                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = "pr_con_set_apidataprocessing";
                    command.CommandTimeout = 0;
                    command.Parameters.AddWithValue("pipelinecode", pipelinecode);
                    command.Parameters.AddWithValue("schedulerid", sched_gid);
                    command.Parameters.AddWithValue("in_datasetcode", datasetcode);
                    command.ExecuteNonQuery();
                    connect.Close();
                    return (sched_gid.ToString());

                }
                else
                {
                    return ("This Pipeline is not scheduled..!");
                }
            }
            ;
        }

        #endregion

        [HttpPost]
        public async Task<IActionResult> UpdateScheduler(int scheduler_gid, string scheduler_status, string initiated_by)
        {
            try
            {
                var shdlr = await dbContext.con_trn_tscheduler.FindAsync(scheduler_gid);

                if (shdlr != null)
                {
                    shdlr.scheduler_status = scheduler_status;
                    shdlr.last_update_date = GetServerDateTime();//DateTime.Now;
                    shdlr.scheduler_initiated_by = initiated_by;

                    await dbContext.SaveChangesAsync();

                    return Ok("Record Updated Successfully");
                }
                else
                {
                    return NotFound("Record Not Found for Update");
                }
            }
            catch (DbUpdateConcurrencyException ex)
            {
                // Handle concurrency conflicts
                // You may want to reload the entity and apply changes again or inform the user about the conflict.
                return Conflict($"Concurrency Conflict: {ex.Message}");
            }
            catch (Exception ex)
            {
                // Handle other exceptions
                return StatusCode(500, $"An error occurred while updating the record: {ex.Message}");
            }
        }

        public DateTime GetServerDateTime()
        {
            using (MySqlConnection connection = new MySqlConnection(targetconnectionString))
            {
                connection.Open(); // Synchronous method to open the connection

                MySqlCommand command = connection.CreateCommand();
                command.CommandText = "SELECT NOW();"; // Query to get current server datetime

                object result = command.ExecuteScalar(); // Synchronous method to execute the query

                if (result != null && DateTime.TryParse(result.ToString(), out DateTime serverDateTime))
                {
                    return serverDateTime;
                }
                else
                {
                    throw new InvalidOperationException("Unable to fetch server datetime.");
                }
            }
        }

        public async Task<string> Reschedulefornexttime_api(string pipelinecode)
        {
            string msg = "Success";
            try
            {
                var finaliz = dbContext.con_trn_tpplfinalization
                         .Where(a => a.pipeline_code == pipelinecode && a.delete_flag == "N")
                         .Select(a => new
                         {
                             finalization_gid = a.finalization_gid,
                             cron_expression = a.cron_expression,
                             pipeline_code = a.pipeline_code
                         }).OrderByDescending(a => a.finalization_gid)
                         .FirstOrDefault();


                //Insert on Scheduler table once pipeline activated
                var schldpplcode = dbContext.con_trn_tscheduler
                     .Where(a => a.pipeline_code == pipelinecode
                     //&& a.scheduler_status == "Scheduled" 
                     && ( a.scheduler_status == "Scheduled" || a.scheduler_status == "Locked" )
                     && a.delete_flag == "N")
                     .Select(a => new
                     {
                         scheduler_gid = a.scheduler_gid,
                         pipeline_code = a.pipeline_code,
                         dataset_code = a.dataset_code,
                         Rawfilepath = a.file_path
                     }).OrderByDescending(a => a.scheduler_gid)
                     .FirstOrDefault();

                if (schldpplcode == null)
                {
                    DateTime v_scheduler_start_date = Convert.ToDateTime(GetNextFireTime(finaliz.cron_expression));
                    var sch = new Scheduler()
                    {
                        scheduler_gid = 0,
                        scheduled_date = GetServerDateTime(),//DateTime.Now,
                        pipeline_code = pipelinecode,
                        dataset_code = "",
                        file_path = "",
                        file_name = "",
                        scheduler_start_date = v_scheduler_start_date,//DateTime.Now,
                        scheduler_status = "Scheduled",
                        scheduler_initiated_by = "System",
                        delete_flag = "N"
                    };

                    await dbContext.con_trn_tscheduler.AddAsync(sch);
                    await dbContext.SaveChangesAsync();
                }
                return msg;
            }
            catch (Exception ex)
            {
                msg = "Failed";
                return msg;
            }
        }

        private DateTime? GetNextFireTime(string inputTime)
        {
            var cron = new Quartz.CronExpression(inputTime + " ?");
            var date = GetServerDateTime();//DateTime.Now;
            DateTimeOffset? nextFire = cron.GetNextValidTimeAfter(date);

            // Convert the result to local time if nextFire has a value
            DateTime? localNextFire = nextFire?.LocalDateTime;

            // Log the cron expression, current date, and next fire time
            Console.WriteLine($"Cron Expression: {cron}");
            Console.WriteLine($"Current Date: {date}");
            Console.WriteLine($"Next Fire: {nextFire}");

            return localNextFire;
        }

        public IActionResult ValidateJson([FromBody] JsonInput input)
        {
            try
            {
                string templateDecoded = DecodePossiblyDoubleEncodedJson(input.TemplateJson);
                string dataDecoded = input.DataJson;

                JObject templateObj = JObject.Parse(templateDecoded);
                JObject dataObj = JObject.Parse(dataDecoded);

                List<ValidationError> errors = new List<ValidationError>();

                CompareStructure(templateObj, dataObj, "$", errors);

                if (errors.Count == 0)
                {
                    return Ok(new { valid = true });
                }

                return Ok(new
                {
                    valid = false,
                    errors = errors
                });
            }
            catch (Exception ex)
            {
                return Ok(new { valid = false, message = ex.Message });
            }
        }

        private string DecodePossiblyDoubleEncodedJson(string json)
        {
            json = json.Trim();

            try
            {
                if (json.StartsWith("\"") && json.EndsWith("\""))
                {
                    string decoded = JsonConvert.DeserializeObject<string>(json);

                    if (decoded.Trim().StartsWith("{") || decoded.Trim().StartsWith("["))
                        return decoded;
                }
            }
            catch { }

            return json;
        }

        private void CompareStructure(JToken template, JToken data, string path, List<ValidationError> errors)
        {
            // Template null placeholder
            if (template.Type == JTokenType.Null)
                return;

            // TYPE MISMATCH
            if (template.Type != data.Type)
            {
                errors.Add(new ValidationError
                {
                    Path = path,
                    Message = $"Type mismatch. Template type = {template.Type}, Data type = {data.Type}"
                });
                return;
            }

            // OBJECT
            if (template is JObject tObj && data is JObject dObj)
            {
                var tProps = tObj.Properties().Select(p => p.Name).ToList();
                var dProps = dObj.Properties().Select(p => p.Name).ToList();

                // Missing keys
                foreach (var key in tProps)
                {
                    if (!dProps.Contains(key))
                    {
                        errors.Add(new ValidationError
                        {
                            Path = $"{path}.{key}",
                            Message = $"Key '{key}' is missing in Data JSON."
                        });
                    }
                }

                // Extra keys
                foreach (var key in dProps)
                {
                    if (!tProps.Contains(key))
                    {
                        errors.Add(new ValidationError
                        {
                            Path = $"{path}.{key}",
                            Message = $"Extra key '{key}' exists in Data JSON but not in Template."
                        });
                    }
                }

                // Recurse for nested properties that exist in both
                foreach (var prop in tObj.Properties())
                {
                    if (dObj.Property(prop.Name) != null)
                    {
                        string nextPath = path == "$" ? "$." + prop.Name : path + "." + prop.Name;

                        CompareStructure(prop.Value, dObj[prop.Name], nextPath, errors);
                    }
                }
            }
            // ARRAY
            else if (template is JArray tArr && data is JArray dArr)
            {
                if (!tArr.Any() || !dArr.Any())
                    return;

                string nextPath = path + "[0]";
                CompareStructure(tArr.First(), dArr.First(), nextPath, errors);
            }
        }

        #region Models
        public class ValidationError
        {
            public string Path { get; set; } = "";
            public string Message { get; set; } = "";
        }
        public class JsonInput
        {
            public string TemplateJson { get; set; }
            public string DataJson { get; set; }
        }
        #endregion
    }
}