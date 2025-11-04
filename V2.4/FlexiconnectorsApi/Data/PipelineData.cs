using DocumentFormat.OpenXml.ExtendedProperties;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using MySql.Data.MySqlClient;
using MysqlEfCoreDemo.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using static MysqlEfCoreDemo.Data.UserInfoModel;
using static MysqlEfCoreDemo.Models.PipelineListModel;

namespace MysqlEfCoreDemo.Data
{
    public class PipelineData
    {
        System.Data.DataSet ds = new System.Data.DataSet();
        DataTable result = new DataTable();

        List<IDbDataParameter>? parameters;

        
        public List<DataTable> getSupportingDoclist(List<SupportingDoc> supportingDocList, UserInfoModel.headerValue headerval, string constring)
        {
            List<DataTable> results = new List<DataTable>();

            try
            {
                foreach (var singleDoc in supportingDocList)
                {
                    

                    DBManager dbManager = new DBManager(constring);
                    {
                        List<IDbDataParameter> parameters = new List<IDbDataParameter>();

                        //parameters.Add(dbManager.CreateParameter("in_supportingdoc_gid", singleDoc.supportingdoc_gid, DbType.Int32));

                        var param_gid = dbManager.CreateParameter("in_supportingdoc_gid", singleDoc.supportingdoc_gid, DbType.Int32);
                        param_gid.Direction = ParameterDirection.InputOutput;
                        parameters.Add(param_gid);
                        parameters.Add(dbManager.CreateParameter("in_pipeline_code", singleDoc.pipeline_code, DbType.String));
                        parameters.Add(dbManager.CreateParameter("in_supportingdoc_name", singleDoc.supportingdoc_name, DbType.String));
                        parameters.Add(dbManager.CreateParameter("in_supportingdoc_remarks", singleDoc.supportingdoc_remarks, DbType.String));
                        parameters.Add(dbManager.CreateParameter("in_supportingdoc_size", singleDoc.supportingdoc_size, DbType.String));
                        parameters.Add(dbManager.CreateParameter("in_action", singleDoc.action, DbType.String));
                        parameters.Add(dbManager.CreateParameter("in_action_by", singleDoc.created_by, DbType.String));

                        var outMsg = dbManager.CreateParameter("out_msg", "", DbType.String);
                        outMsg.Direction = ParameterDirection.Output;
                        outMsg.Size = 4000; 
                        parameters.Add(outMsg);

                        var outResult = dbManager.CreateParameter("out_result", 0, DbType.Int32);
                        outResult.Direction = ParameterDirection.Output;
                        parameters.Add(outResult); 
 
                        System.Data.DataSet ds = dbManager.execStoredProcedure("pr_con_trn_tsupportingdoc", CommandType.StoredProcedure, parameters.ToArray());
                        if (ds != null && ds.Tables.Count > 0)
                        {   
                            results.Add(ds.Tables[0]); 
                        }
                        else
                            results.Add(new DataTable());
                    }
                     
                }
            }
            catch (Exception ex)
            {
                CommonHeader objlog = new CommonHeader();
                objlog.logger("SP:pr_con_trn_tsupportingdoc Error: " + ex.Message);
                objlog.commonDataapi("", "SP", ex.Message, "pr_con_trn_tsupportingdoc", headerval.user_code, constring);
            }

            return results;
        }

        public DataTable FetchSupportDoclist(string pipeline_code, string constring)
        {
            //List<DataTable> results = new List<DataTable>();

            DataTable results=new DataTable();
            try
            { 
                DBManager dbManager = new DBManager(constring);
                {
                    List<IDbDataParameter> parameters = new List<IDbDataParameter>();

                    parameters.Add(dbManager.CreateParameter("in_supportingdoc_gid", pipeline_code, 0));
                    parameters.Add(dbManager.CreateParameter("in_pipeline_code", pipeline_code, DbType.String));
                     parameters.Add(dbManager.CreateParameter("in_action","get", DbType.String)); 

                    System.Data.DataSet ds = dbManager.execStoredProcedure("pr_con_trn_tgetsupportingdoc", CommandType.StoredProcedure, parameters.ToArray());
                      results=(ds.Tables[0]);
                    return results;
                }
            }
            catch (Exception ex)
            {
                CommonHeader objlog = new CommonHeader();
                objlog.logger("SP:pr_con_trn_tgetsupportingdoc Error: " + ex.Message);
                objlog.commonDataapi("", "SP", ex.Message, "pr_con_trn_tgetsupportingdoc", pipeline_code, constring);
            }
            return results;
        }

        public DataTable deleteSupportDocs(Int32 id, string constring)
        {
            //List<DataTable> results = new List<DataTable>();
          
          DataTable results = new DataTable();
            try
            {
                DBManager dbManager = new DBManager(constring);
                {
                    List<IDbDataParameter> parameters = new List<IDbDataParameter>();

                    // Input/Output parameter for GID (used in insert/update cases)
                    var param_gid = dbManager.CreateParameter("in_supportingdoc_gid",id, DbType.Int32);
                    param_gid.Direction = ParameterDirection.InputOutput;
                    parameters.Add(param_gid); 
                    // Input parameters
                    parameters.Add(dbManager.CreateParameter("in_pipeline_code","", DbType.String));
                    parameters.Add(dbManager.CreateParameter("in_supportingdoc_name","", DbType.String)); 
                    parameters.Add(dbManager.CreateParameter("in_supportingdoc_remarks","", DbType.String));
                    parameters.Add(dbManager.CreateParameter("in_supportingdoc_size","", DbType.String));
                    parameters.Add(dbManager.CreateParameter("in_action", "delete", DbType.String));  // INSERT / UPDATE / DELETE
                    parameters.Add(dbManager.CreateParameter("in_action_by","system", DbType.String));
                     
                    var outMsg = dbManager.CreateParameter("out_msg", "", DbType.String);
                    outMsg.Direction = ParameterDirection.Output;
                    outMsg.Size = 4000;
                    parameters.Add(outMsg);

                    var outResult = dbManager.CreateParameter("out_result", 0, DbType.Int32);
                    outResult.Direction = ParameterDirection.Output;
                    parameters.Add(outResult); 
                    System.Data.DataSet ds = dbManager.execStoredProcedure("pr_con_trn_tsupportingdoc", CommandType.StoredProcedure, parameters.ToArray());
                    int resultCode = Convert.ToInt32(outResult.Value); 
                    results=  (ds.Tables[0]);
                }

            }
            catch (Exception ex)
            {
                CommonHeader objlog = new CommonHeader();
                objlog.logger("SP:pr_con_trn_tsupportingdoc Error: " + ex.Message);
                objlog.commonDataapi("", "SP", ex.Message, "pr_con_trn_tsupportingdoc",id.ToString(), constring);
            }
            return results;
        }




    }

}
