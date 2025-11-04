using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using MysqlEfCoreDemo.Data;
using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

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
        #endregion

        private readonly IConfiguration _configuration;
        private readonly MyDbContext dbContext;
        public SchedulerController(MyDbContext dbContext, IConfiguration configuration)
        {
            _configuration = configuration;

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
            this.dbContext = dbContext;

        }

        [HttpGet]
        public async Task<IActionResult> GetSchedulerPath(string pipeline_code, string dataset_code)
        {
            try
            {
                // Check if pipeline exists in the main table
                var pipelineExists = await dbContext.con_mst_tpipeline
                    .AnyAsync(p => p.pipeline_code == pipeline_code && p.pipeline_status == "Active"  && p.delete_flag == "N");

                if (!pipelineExists)
                    return NotFound("This pipeline is not Active..!");

                // Get scheduler_path from the pipeline detail table
                var pipelineDetail = await dbContext.con_trn_tpipelinedetails
                    .Where(d => d.pipeline_code == pipeline_code
                                && d.target_dataset_code == dataset_code
                                && d.pipelinedet_status == "Active"
                                && d.delete_flag == "N")
                    .Select(d => new { d.scheduler_path })
                    .FirstOrDefaultAsync();

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


    }
}
