using System;

namespace MysqlEfCoreDemo.Models
{
    public class DatasetModel
    {
        public class Datasetlistmodel
        {
            public string? dataset_name { get; set; }
            public string? datasetCode { get; set; }
            public int dataset_id { get; set; }
            public string? dataset_category { get; set; }
            public string? active_status { get; set; }
            public string? active_status_desc { get; set; }
            public int in_user_gid { get; set; }
            public string? in_user_code { get; set; }
            public string? in_active_status { get; set; }
        }
        public class DatasetHeadermodel
        {
            public string dataset_systemflag { get; set; }
            public string pipeline_code { get; set; }
            public Int32 pipelinedet_id { get; set; }
            public string? dataset_name { get; set; }
            public string? datasetCode { get; set; }
            public Int32 dataset_id { get; set; }
            public string? dataset_category { get; set; }
            public string? clone_dataset { get; set; }
            public string? active_status { get; set; }
            public string? inactive_reason { get; set; }
            public string pipelinedet_dataset_type { get; set; }
            public string? in_action { get; set; }
            public string? in_action_by { get; set; }
            public string? out_msg { get; set; }
            public string? out_result { get; set; }
        }
        public class Datasetdetailmodel
        {
            public string? datasetCode { get; set; }
            public int datasetdetail_id { get; set; }
            public string? field_name { get; set; }
            public string? field_type { get; set; }
            public string? field_length { get; set; }
            public int? precision_length { get; set; }
            public int? scale_length { get; set; }
            public string? field_mandatory { get; set; }
            public decimal? dataset_seqno { get; set; }
            public string? in_action { get; set; }
            public string? in_action_by { get; set; }
            public string? out_msg { get; set; }
            public string? out_result { get; set; }
        }
        public class Datasetdetailmodellist
        {
            public Int16 dataset_gid { get; set; }
        }

        public class clonedataset
        {
            public string? in_clone_dataset_code { get; set; }
            public string? in_new_dataset_code { get; set; }
            public string? in_action_by { get; set; }

        }

        public class getSchedulerModel
        {
            public DateOnly in_processed_date { get; set; }
            public string? in_scheduler_status { get; set; }
        }

        public class delSchedulerModel
        {
            public int in_scheduler_gid { get; set; }
            public string in_remark { get; set; }
        }

        public class datasetAgainstReconModel
        {
            public string in_recon_code { get; set; }
        }

        public class getaccbaldatasetModel
        {
            public string in_dataset_code { get; set; }

        }

        public class setAccountbalanceModel
        {
            public int in_accbal_gid { get; set; }
            public String in_dataset_code { get; set; }
            public DateOnly? in_tran_date { get; set; }
            public Double? in_bal_value { get; set; }
            public String in_action { get; set; }
        }
        public class datasethistoryModel
        {
            public string in_dataset_code { get; set; }
        }
    }
}
