using DocumentFormat.OpenXml.ExtendedProperties;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using static MysqlEfCoreDemo.Models.DatasetModel;

namespace MysqlEfCoreDemo.Data
{
    public class DatasetData
    {
        DataSet ds = new DataSet();
        DataTable result = new DataTable();

        List<IDbDataParameter>? parameters;

        public DataTable GetDatasetdata(string pipelinecode, UserInfoModel.headerValue headerval, string constring, string source_fields = "", string datasetType = "")
        {
            try
            {
                DBManager dbManager = new DBManager(constring);
                Dictionary<string, Object> values = new Dictionary<string, object>();
                parameters = new List<IDbDataParameter>();
                parameters.Add(dbManager.CreateParameter("in_pipeline_code", pipelinecode, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_source_fields", source_fields, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_dataset_type", datasetType, DbType.String));
                ds = dbManager.execStoredProcedure("pr_con_get_Dataset", CommandType.StoredProcedure, parameters.ToArray());
                result = ds.Tables[0];
                return result;
            }
            catch (Exception ex)
            {
                CommonHeader objlog = new CommonHeader();
                objlog.logger("SP:pr_con_get_Dataset" + "Error Message:" + ex.Message);
                objlog.commonDataapi("", "SP", ex.Message + "Param:" + JsonConvert.SerializeObject(pipelinecode), "pr_con_get_Dataset", headerval.user_code, constring);
                return result;
            }
        }
        public DataTable GetDatasetFielddata(string pipelinecode, string datasetcode,UserInfoModel.headerValue headerval, string constring)
        {
            try
            {
                DBManager dbManager = new DBManager(constring);
                Dictionary<string, Object> values = new Dictionary<string, object>();
                parameters = new List<IDbDataParameter>();
                parameters.Add(dbManager.CreateParameter("in_pipeline_code", pipelinecode, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_dataset_code", datasetcode, DbType.String));
                ds = dbManager.execStoredProcedure("pr_con_get_Datasetfield", CommandType.StoredProcedure, parameters.ToArray());
                result = ds.Tables[0];
                return result;
            }
            catch (Exception ex)
            {
                CommonHeader objlog = new CommonHeader();
                objlog.logger("SP:pr_con_get_Datasetfield" + "Error Message:" + ex.Message);
                objlog.commonDataapi("", "SP", ex.Message + "Param:" + JsonConvert.SerializeObject(pipelinecode), "$(\"#txtds_code\")", headerval.user_code, constring);
                return result;
            }
        }


        public DataTable DatasetHeaderdata(DatasetHeadermodel Objmodel, UserInfoModel.headerValue headerval, string constring)
        {
            try
            {
                DBManager dbManager = new DBManager(constring);
                Dictionary<string, Object> values = new Dictionary<string, object>();
                parameters = new List<IDbDataParameter>();
                parameters.Add(dbManager.CreateParameter("in_pipeline_code", Objmodel.pipeline_code, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_pipelinedet_gid", Objmodel.pipelinedet_id, DbType.Int32, ParameterDirection.InputOutput));
                parameters.Add(dbManager.CreateParameter("in_dataset_systemflag", Objmodel.dataset_systemflag, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_dataset_gid", Objmodel.dataset_id, DbType.Int32, ParameterDirection.InputOutput));
                parameters.Add(dbManager.CreateParameter("in_dataset_code", Objmodel.datasetCode, DbType.String, ParameterDirection.InputOutput));
                parameters.Add(dbManager.CreateParameter("in_dataset_name", Objmodel.dataset_name, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_dataset_category", Objmodel.dataset_category, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_clone_dataset", Objmodel.clone_dataset, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_active_status", Objmodel.active_status, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_action", Objmodel.in_action, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_action_by", headerval.user_code, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_active_reason", Objmodel.inactive_reason, DbType.String)); 
                parameters.Add(dbManager.CreateParameter("in_pipelinedet_dataset_type", Objmodel.pipelinedet_dataset_type, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_user_code", headerval.user_code, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_role_code", headerval.role_code, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_lang_code", headerval.lang_code, DbType.String));
                parameters.Add(dbManager.CreateParameter("out_msg", "out", DbType.String, ParameterDirection.Output));
                parameters.Add(dbManager.CreateParameter("out_result", "out", DbType.String, ParameterDirection.Output));
                ds = dbManager.execStoredProcedure("pr_con_set_Dataset", CommandType.StoredProcedure, parameters.ToArray());
                result = ds.Tables[0];
                return result;
            }
            catch (Exception ex)
            {
                CommonHeader objlog = new CommonHeader();
                objlog.logger("SP:pr_con_set_Dataset" + "Error Message:" + ex.Message);
                objlog.commonDataapi("", "SP", ex.Message + "Param:" + JsonConvert.SerializeObject(Objmodel), "pr_con_set_Dataset", headerval.user_code, constring);
                return result;
            }
        }

        public DataTable DatasetDetaildata(Datasetdetailmodel Objmodel, UserInfoModel.headerValue headerval, string constring)
        {
            try
            {
                DBManager dbManager = new DBManager(constring);
                Dictionary<string, Object> values = new Dictionary<string, object>();
                parameters = new List<IDbDataParameter>();
                parameters.Add(dbManager.CreateParameter("in_datasetfield_gid", SafeDbValue(Objmodel.datasetdetail_id), DbType.Int64, ParameterDirection.InputOutput));
                parameters.Add(dbManager.CreateParameter("in_dataset_code", SafeDbValue(Objmodel.datasetCode), DbType.String));
                parameters.Add(dbManager.CreateParameter("in_field_name", SafeDbValue(Objmodel.field_name), DbType.String));
                parameters.Add(dbManager.CreateParameter("in_field_type", SafeDbValue(Objmodel.field_type), DbType.String));
                parameters.Add(dbManager.CreateParameter("in_field_length", SafeDbValue(Objmodel.field_length), DbType.String));
                parameters.Add(dbManager.CreateParameter("in_precision_length", SafeDbValue(Objmodel.precision_length), DbType.Int64));
                parameters.Add(dbManager.CreateParameter("in_scale_length", SafeDbValue(Objmodel.scale_length), DbType.Int64));
                parameters.Add(dbManager.CreateParameter("in_field_mandatory", SafeDbValue(Objmodel.field_mandatory), DbType.String));
                parameters.Add(dbManager.CreateParameter("in_dataset_seqno", SafeDbValue(Objmodel.dataset_seqno), DbType.Decimal));
                parameters.Add(dbManager.CreateParameter("in_action", SafeDbValue(Objmodel.in_action), DbType.String));
                parameters.Add(dbManager.CreateParameter("in_action_by", SafeDbValue(headerval.user_code), DbType.String));
                parameters.Add(dbManager.CreateParameter("in_user_code", SafeDbValue(headerval.user_code), DbType.String));
                parameters.Add(dbManager.CreateParameter("in_role_code", SafeDbValue(headerval.role_code), DbType.String));
                parameters.Add(dbManager.CreateParameter("in_lang_code", SafeDbValue(headerval.lang_code), DbType.String));

                // Output parameters (don't need SafeDbValue because you're initializing them)
                parameters.Add(dbManager.CreateParameter("out_dataset_table_field", "out", DbType.String, ParameterDirection.Output));
                parameters.Add(dbManager.CreateParameter("out_msg", "out", DbType.String, ParameterDirection.Output));
                parameters.Add(dbManager.CreateParameter("out_result", "out", DbType.String, ParameterDirection.Output));

                ds = dbManager.execStoredProcedure("pr_con_set_tdatasetfield", CommandType.StoredProcedure, parameters.ToArray());
                result = ds.Tables[0];
                return result;
            }
            catch (Exception ex)
            {
                CommonHeader objlog = new CommonHeader();
                objlog.logger("SP:pr_con_set_tdatasetfield" + "Error Message:" + ex.Message);
                objlog.commonDataapi("", "SP", ex.Message + "Param:" + JsonConvert.SerializeObject(Objmodel), "pr_con_set_tdatasetfield", headerval.user_code, constring);
                return result;
            }
        }

        private object SafeDbValue(object value)
        {
            return value ?? DBNull.Value;
        }

        //ClonePipelineDatasetData
        public DataTable ClonePipelineDatasetData(ClonePipelineDatasetModel objClonePipelineDatasetModel, UserInfoModel.headerValue headerval, string constring)
        {
            try
            {
                DBManager dbManager = new DBManager(constring);
                Dictionary<string, Object> values = new Dictionary<string, object>();
                parameters = new List<IDbDataParameter>();
                parameters.Add(dbManager.CreateParameter("in_pipeline_code", SafeDbValue(objClonePipelineDatasetModel.in_pipeline_code), DbType.String));
                parameters.Add(dbManager.CreateParameter("in_clone_dataset_code", SafeDbValue(objClonePipelineDatasetModel.in_clone_dataset_code), DbType.String));
                parameters.Add(dbManager.CreateParameter("in_new_dataset_code", SafeDbValue(objClonePipelineDatasetModel.in_new_dataset_code), DbType.String));
                parameters.Add(dbManager.CreateParameter("in_user_code", SafeDbValue(objClonePipelineDatasetModel.in_user_code), DbType.String));
                parameters.Add(dbManager.CreateParameter("out_msg", "out", DbType.String, ParameterDirection.Output));
                parameters.Add(dbManager.CreateParameter("out_result", "out", DbType.String, ParameterDirection.Output));
                ds = dbManager.execStoredProcedure("pr_con_pipeline_dataset_clone", CommandType.StoredProcedure, parameters.ToArray());
                result = ds.Tables[0];
                return result;
            }
            catch (Exception ex)
            {
                CommonHeader objlog = new CommonHeader();
                objlog.logger("SP:pr_con_pipeline_dataset_clone" + "Error Message:" + ex.Message);
                objlog.commonDataapi("", "SP", ex.Message + "Param:" + JsonConvert.SerializeObject(objClonePipelineDatasetModel), "pr_con_pipeline_dataset_clone", headerval.user_code, constring);
                return result;
            }
        }

        //getAllDatasetFieldsData
        public DataTable getAllDatasetFieldsData(getAllDatasetFieldsModel objgetAllDatasetFields, UserInfoModel.headerValue headerval, string constring)
        {
            try
            {
                DBManager dbManager = new DBManager(constring);
                Dictionary<string, Object> values = new Dictionary<string, object>();
                parameters = new List<IDbDataParameter>();
                parameters.Add(dbManager.CreateParameter("in_pipeline_code", SafeDbValue(objgetAllDatasetFields.in_pipeline_code), DbType.String));
                parameters.Add(dbManager.CreateParameter("in_dataset_code", SafeDbValue(objgetAllDatasetFields.in_dataset_code), DbType.String));
                ds = dbManager.execStoredProcedure("pr_con_get_all_datasetfields", CommandType.StoredProcedure, parameters.ToArray());
                result = ds.Tables[0];
                return result;
            }
            catch (Exception ex)
            {
                CommonHeader objlog = new CommonHeader();
                objlog.logger("SP:pr_con_get_all_datasetfields" + "Error Message:" + ex.Message);
                objlog.commonDataapi("", "SP", ex.Message + "Param:" + JsonConvert.SerializeObject(objgetAllDatasetFields), "pr_con_get_all_datasetfields", headerval.user_code, constring);
                return result;
            }
        }

        // GetparentchildRelationlist

        public DataSet GetparentchildRelationlist(string pipeline_code, string dataset_code, string child_ds_code, UserInfoModel.headerValue headerval, string constring)
        {
            try
            {
                DBManager dbManager = new DBManager(constring);
                Dictionary<string, Object> values = new Dictionary<string, object>();
                parameters = new List<IDbDataParameter>();
                parameters.Add(dbManager.CreateParameter("in_pipeline_code", pipeline_code, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_parent_ds_code", dataset_code, DbType.String));
                parameters.Add(dbManager.CreateParameter("in_child_ds_code", child_ds_code, DbType.String));
                ds = dbManager.execStoredProcedure("pr_con_get_parentchildRelationlist", CommandType.StoredProcedure, parameters.ToArray());
                //result = ds.Tables[0];
                return ds;
            }
            catch (Exception ex)
            {
                CommonHeader objlog = new CommonHeader();
                objlog.logger("SP:pr_con_get_parentchildRelationlist" + "Error Message:" + ex.Message);
                objlog.commonDataapi("", "SP", ex.Message + "Param:" + JsonConvert.SerializeObject(pipeline_code), "$(\"#txtds_code\")", headerval.user_code, constring);
                return ds;
            }
        }
    }
}
