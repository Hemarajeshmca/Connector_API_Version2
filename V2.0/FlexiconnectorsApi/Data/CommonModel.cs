namespace MysqlEfCoreDemo.Data
{
    public class CommonModel
    {
        public class errorlogModel
        {
            public string in_ip_addr { get; set; }
            public string in_source_name { get; set; }
            public string in_proc_name { get; set; }
            public string in_errorlog_text { get; set; }
            public string user_code { get; set; }

        }

        public class configvalueModel
        {
            public string in_config_name { get; set; }

        }
    }
}
