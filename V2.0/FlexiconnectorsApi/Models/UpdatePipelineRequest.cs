using Microsoft.AspNetCore.Http;
using System;
using System.ComponentModel.DataAnnotations;
using System.Reflection.Metadata;

namespace MysqlEfCoreDemo.Models
{
    public class UpdatePipelineRequest
    {
        public string pipeline_gid { get; set; }
        public string pipeline_code { get; set; }
        public string pipeline_name { get; set; }
        public string pipeline_desc { get; set; }
        public string connection_code { get; set; }
        public string connection_name { get; set; }
        public string db_name { get; set; }
        public string source_file_name { get; set; }
        //public string macro_file_name { get; set; }
        public string sheet_name { get; set; }
        public string dataset_name { get; set; }
        public string table_view_query_type { get; set; }
        public string table_view_query_desc { get; set; }
        public string target_dataset_code { get; set; }
        public string pipeline_status { get; set; }
        public DateTime created_date { get; set; }
        public string created_by { get; set; }
        public DateTime updated_date { get; set; }
        public string updated_by { get; set; }
    }
    public class UpdatepplSourceFieldRequest
    {
        public string pplsourcefield_gid { get; set; }
        public string pipeline_code { get; set; }
        public string dataset_code { get; set; }
        public string sourcefield_name { get; set; }
        public int sourcefield_sno { get; set; }
        public string sourcefield_datatype { get; set; }
        public string sourcefield_expression { get; set; }
        public string source_type { get; set; }
        public DateTime created_date { get; set; }
        public string created_by { get; set; }
        public DateTime updated_date { get; set; }
        public string updated_by { get; set; }
        public string delete_flag { get; set; }
    }
    public class UpdatepplFieldMappingRequest
    {
        public string pplfieldmapping_gid { get; set; }
        public string pipeline_code { get; set; }
        public int pplfieldmapping_flag { get; set; }
        public string dataset_code { get; set; }
        public string ppl_field_name { get; set; }
        public string dataset_field_name { get; set; }
        public string default_value { get; set; }
        public DateTime created_date { get; set; }
        public string created_by { get; set; }
        public DateTime updated_date { get; set; }
        public string updated_by { get; set; }
        public string delete_flag { get; set; }
        public string pplsourcefield_format { get; set; }
        public string has_saved { get; set; }
        public string system_flag { get; set; }
        public string mapped_ppl_field_name { get; set; }
        public string field_mapped { get; set; }
        public string dataset_gid { get; set; }
        public string datasetfield_gid { get; set; }        
        public string datasetdetail_id { get; set; }
        public string dataset_code1 { get; set; }
        public string dataset_seqno { get; set; }
        public string field_name { get; set; }
        public string field_type { get; set; }
        public string field_length { get; set; }
        public string precision_length { get; set; }
        public string scale_length { get; set; }
        public string field_mandatory { get; set; }
        public string dataset_field_sno { get; set; }
        public string dataset_table_field { get; set; }
        public string Status { get; set; }
        public string in_action { get; set; }
        public char its_parent_pipeline { get; set; }
        public string parent_pipeline_code { get; set; }
        public string parent_pipeline_name { get; set; }

    }
    public class UpdatepplConditionRequest
    {
        public string pplcondition_gid { get; set; }
        public string pipeline_code { get; set; }
        public string condition_type { get; set; }
        public string condition_name { get; set; }
        public string condition_text { get; set; }
        public string condition_msg { get; set; }
		public DateTime created_date { get; set; }
        public string created_by { get; set; }
        public DateTime updated_date { get; set; }
        public string updated_by { get; set; }
        public string delete_flag { get; set; }
    }
    public class UpdatepplFinalizationRequest
    {
        public string finalization_gid { get; set; }
        public string pipeline_code { get; set; }
        public string run_type { get; set; }
        public string cron_expression { get; set; }
        public string extract_mode { get; set; }
        public string duplicate_mode { get; set; }
        public string upload_mode { get; set; }
        public string key_field { get; set; }
        public string extract_condition { get; set; }
        public string last_incremental_val { get; set; }
        public string parent_dataset_code { get; set; }
        public int pull_days { get; set; } 
        public string reject_duplicate_flag { get; set; }
        public string error_mode { get; set; }
        public DateTime created_date { get; set; }
        public string created_by { get; set; }
        public DateTime updated_date { get; set; }
        public string updated_by { get; set; }
        public string delete_flag { get; set; }
        public string pipeline_status { get; set; }
    }

    public class UpdatepplCsvHeaderRequest
    {
        public string pplcsvheader_gid { get; set; }
        public string pipeline_code { get; set; }
        public string column_separator { get; set; }
        public int number_ofcolumns { get; set; }
        public int number_oflines_toskip { get; set; }
        public string csvfile_dateformat { get; set; }
        public string csvfile_datetimeformat { get; set; }
        public DateTime created_date { get; set; }
        public string created_by { get; set; }
        public DateTime updated_date { get; set; }
        public string updated_by { get; set; }
        public string delete_flag { get; set; }
    }


    public class UpdatepplApiHeaderRequest
    {
        public int api_gid { get; set; }
        public string pipeline_code { get; set; }
        public string api_jsonvalue { get; set; }
        public string api_key { get; set; }
        public string api_value { get; set; }
        public string remarks { get; set; }
        public DateTime created_date { get; set; }
        public string created_by { get; set; }
        public DateTime updated_date { get; set; }
        public string updated_by { get; set; }
        public string delete_flag { get; set; }

    }
}
