using System;

namespace MysqlEfCoreDemo.Models
{
    public class UpdateConnectorRequest
    {
        public string connection_gid { get; set; }
        public string connection_code { get; set; }
        public string connection_name { get; set; }
        public string connection_desc { get; set; }
        public string source_db_type { get; set; }
        public string protection_type { get; set; }
        public string file_password { get; set; }
        public string source_host_name { get; set; }
        public string source_port { get; set; }
        public string source_auth_mode { get; set; }
        public string source_db_user { get; set; }
        public string source_db_pwd { get; set; }
        public string source_auth_file_name { get; set; }
        public byte[] source_auth_file_blob { get; set; }
        public string having_auth_url { get; set; }
        public string source_file { get; set; }
        public string ssh_tunneling { get; set; }
        public string ssh_host_name { get; set; }
        public string ssh_port { get; set; }
        public string ssh_user { get; set; }
        public string ssh_pwd { get; set; }
        public string ssh_auth_mode { get; set; }
        public string ssh_file_name { get; set; }
        public byte[] ssh_file_blob { get; set; }
        public string connection_status { get; set; }
        public DateTime created_date { get; set; }
        public string created_by { get; set; }
        public DateTime updated_date { get; set; }
        public string updated_by { get; set; }
    }

    public class UpdateapiauthTokenModel
    {
        public string apiauthtoken_gid { get; set; }
        public string connection_code { get; set; }
        public string auth_token_keyname { get; set; }
        public string auth_url { get; set; }
        public string auth_method { get; set; }
        public string auth_token { get; set; }
        public string auth_header_json { get; set; }
        public string auth_payload_type { get; set; }
        public string auth_body_format { get; set; }
        public string auth_payload_json { get; set; }
        public string auth_response { get; set; }
        public string remarks { get; set; }
        public DateTime created_date { get; set; }
        public string created_by { get; set; }
        public DateTime updated_date { get; set; }
        public string updated_by { get; set; }
        public string delete_flag { get; set; }
    }
}
