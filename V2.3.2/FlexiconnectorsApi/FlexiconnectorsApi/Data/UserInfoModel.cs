namespace MysqlEfCoreDemo.Data
{
    public class UserInfoModel
    {
        public class headerValue
        {
            public string user_code { get; set; } = "";
            public string role_code { get; set; } = "";
            public string lang_code { get; set; } = "";
            public string ip_address { get; set; } = "";
        }
    }
}
