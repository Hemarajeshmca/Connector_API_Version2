using System.Collections.Generic;
using System.Data;

namespace MysqlEfCoreDemo.Data
{
    public class DBManager
    {
        private DatabaseHandlerFactory dbFactory;
        private IDatabaseHandler database;
        private string providerName;

        DataTable outParameterTable = new DataTable();

        public DBManager(string connectionStringName)
        {
            dbFactory = new DatabaseHandlerFactory(connectionStringName);
            database = dbFactory.CreateDatabase();
            providerName = dbFactory.GetProviderName();
        }

        public IDbConnection GetDatabasecOnnection()
        {
            return database.CreateConnection();
        }

        public void CloseConnection(IDbConnection connection)
        {
            database.CloseConnection(connection);
        }

        public IDbDataParameter CreateParameter(string name, object value, DbType dbType)
        {
            return DataParameterManager.CreateParameter(providerName, name, value, dbType, ParameterDirection.Input);
        }

        public IDbDataParameter CreateParameter(string name, object value, DbType dbType, ParameterDirection direction)
        {
            return DataParameterManager.CreateParameter(providerName, name, value, dbType, direction);
        }

        public DataSet execStoredProcedure(string commandText, CommandType commandType, IDbDataParameter[]? parameters = null)
        {
            var dynamicData = new Dictionary<string, object>();
            using (var connection = database.CreateConnection())
            {
                connection.Open();
                using (var command = database.CreateCommand(commandText, commandType, connection))
                {
                    if (parameters != null)
                    {
                        foreach (var parameter in parameters)
                        {
                            command.Parameters.Add(parameter);
                            command.CommandTimeout = 0;
                        }
                    }

                    var dataset = new DataSet();
                    var dataAdaper = database.CreateAdapter(command);

                    dataAdaper.Fill(dataset);

                    if (parameters != null)
                    {
                        foreach (var parameter in parameters)
                        {
                            if (parameter != null && (parameter.Direction == ParameterDirection.Output || parameter.Direction == ParameterDirection.InputOutput))
                            {

                                dynamicData.Add(parameter.ParameterName, parameter.Value);
                            }
                        }
                    }
                    outParameterTable = ConvertDictionaryToDataTable(dynamicData);
                    outParameterTable.TableName = "outparam";

                    dataset.Tables.Add(outParameterTable);

                    return dataset;
                }
            }
        }


        public DataSet execStoredProcedurelist(string commandText, CommandType commandType, IDbDataParameter[]? parameters = null)
        {
            var dynamicData = new Dictionary<string, object>();
            using (var connection = database.CreateConnection())
            {
                connection.Open();
                using (var command = database.CreateCommand(commandText, commandType, connection))
                {
                    if (parameters != null)
                    {
                        foreach (var parameter in parameters)
                        {
                            command.Parameters.Add(parameter);
                            command.CommandTimeout = 0;
                        }
                    }

                    var dataset = new DataSet();
                    var dataAdaper = database.CreateAdapter(command);
                    dataAdaper.Fill(dataset);
                    return dataset;
                }
            }
        }




        public DataTable ConvertDictionaryToDataTable(System.Collections.Generic.Dictionary<string, object> dictionary)
        {
            DataTable dataTable = new DataTable();

            // Add columns to the DataTable based on the dictionary keys
            foreach (var key in dictionary.Keys)
            {
                dataTable.Columns.Add(key, dictionary[key].GetType());
            }

            // Add a row to the DataTable using dictionary values
            DataRow row = dataTable.NewRow();
            foreach (var key in dictionary.Keys)
            {
                row[key] = dictionary[key];
            }
            dataTable.Rows.Add(row);

            return dataTable;
        }
    }
}
