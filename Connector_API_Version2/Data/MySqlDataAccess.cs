using MySql.Data.MySqlClient;
using System.Data;

namespace MysqlEfCoreDemo.Data
{
    public class MySqlDataAccess : IDatabaseHandler
    {
        private string ConnectionString { get; set; }

        public MySqlDataAccess(string connectionString)
        {
            ConnectionString = connectionString;
        }
        public IDbConnection CreateConnection()
        {
            return new MySqlConnection(ConnectionString);
        }

        public void CloseConnection(IDbConnection connection)
        {
            var mysqlConnection = (MySqlConnection)connection;
            mysqlConnection.Close();
            mysqlConnection.Dispose();
        }

        public IDbCommand CreateCommand(string commandText, CommandType commandType, IDbConnection connection)
        {
            return new MySqlCommand
            {
                CommandText = commandText,
                Connection = (MySqlConnection)connection,
                CommandType = commandType
            };
        }

        public IDataAdapter CreateAdapter(IDbCommand command)
        {
            return new MySqlDataAdapter((MySqlCommand)command);
        }

        public IDbDataParameter CreateParameter(IDbCommand command)
        {
            MySqlCommand MySqlCommand = (MySqlCommand)command;
            return MySqlCommand.CreateParameter();
        }
    }
}
