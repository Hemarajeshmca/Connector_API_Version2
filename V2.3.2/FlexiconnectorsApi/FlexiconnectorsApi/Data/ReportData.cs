using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;

namespace MysqlEfCoreDemo.Data
{
    public class ReportData
    {

        DataSet ds = new DataSet();
        DataTable result = new DataTable();

        List<IDbDataParameter>? parameters;

        public DataTable PPLDSmappedData(string pipelinecode, string datasetcode, UserInfoModel.headerValue headerval, string constring)
        {
            try
            {
                DBManager dbManager = new DBManager(constring);
                Dictionary<string, Object> values = new Dictionary<string, object>();
                parameters = new List<IDbDataParameter>();
                parameters.Add(dbManager.CreateParameter("in_pipeline_code", pipelinecode, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_dataset_code", datasetcode, DbType.String));
                ds = dbManager.execStoredProcedure("pr_con_pipelinedsmapped_report", CommandType.StoredProcedure, parameters.ToArray());
                result = ds.Tables[0];
                return result;
            }
            catch (Exception ex)
            {
                CommonHeader objlog = new CommonHeader();
                objlog.logger("SP:pr_con_pipelinedsmapped_report" + "Error Message:" + ex.Message);
                objlog.commonDataapi("", "SP", ex.Message + "Param:" + JsonConvert.SerializeObject(pipelinecode), "$(\"#txtds_code\")", headerval.user_code, constring);
                return result;
            }
        }

        public DataTable FileimportData(string fromdate, string todate, UserInfoModel.headerValue headerval, string constring)
        {
            try
            {
                DBManager dbManager = new DBManager(constring);
                Dictionary<string, Object> values = new Dictionary<string, object>();
                parameters = new List<IDbDataParameter>();
                parameters.Add(dbManager.CreateParameter("in_import_from", fromdate, DbType.DateTime));
                parameters.Add(dbManager.CreateParameter("in_import_to", todate, DbType.DateTime));
                ds = dbManager.execStoredProcedure("pr_con_fileimport_report", CommandType.StoredProcedure, parameters.ToArray());
                result = ds.Tables[0];
                return result;
            }
            catch (Exception ex)
            {
                CommonHeader objlog = new CommonHeader();
                objlog.logger("SP:pr_con_fileimport_report" + "Error Message:" + ex.Message);
                objlog.commonDataapi("", "SP", ex.Message + "Param:" + JsonConvert.SerializeObject(fromdate), "$(\"#txtds_code\")", headerval.user_code, constring);
                return result;
            }
        }
    }
}
