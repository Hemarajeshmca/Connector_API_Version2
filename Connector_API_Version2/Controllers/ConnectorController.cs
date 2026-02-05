using Cake.Core.IO;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using MysqlEfCoreDemo.Data;
using MysqlEfCoreDemo.Models;
using Npgsql;
using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using ConnectionInfo = Renci.SshNet.ConnectionInfo;

namespace MysqlEfCoreDemo.Controllers
{
    //[ApiController]
    //[Route("api/[Controller]")]
    public class ConnectorController : ControllerBase
    {
        private readonly MyDbContext dbContext;
        private readonly IConfiguration _configuration;
        string connectionString = null;
        string hostingfor = "";
        string _slash = "";
        MySqlConnection mysqlcnn;
        NpgsqlConnection postgrescnn;

        string errorlogfilePath = "";//"D:\\Mohan\\error_log.txt";
        string errormsg = "";
        public ConnectorController(MyDbContext dbContext, IConfiguration configuration)
        {
            _configuration = configuration;
            hostingfor = _configuration["HostingFor"];//_configuration.GetConnectionString("HostingFor");
            if (hostingfor.Trim() == "Linux")
            {
                _slash = "/";
            }
            else
            {
                _slash = "\\";
            }
            errorlogfilePath = System.IO.Path.Combine(Directory.GetCurrentDirectory(), "Errorlog") + _slash + "error_log.txt";//"D:\\Mohan\\error_log.txt";
            this.dbContext = dbContext;

        }

        [HttpGet]
        public async Task<IActionResult> GetConnectors()
        {
            try
            {
                return Ok(await dbContext.con_mst_tconnection
                    .Where(item => item.delete_flag == "N") // Filter items where delete_flag is 'N'
                    .OrderByDescending(item => item.connection_gid) // Order by connection_gid in descending order
                    .ToListAsync());
                // return Ok(await dbContext.con_mst_tconnection.OrderByDescending(item => item.connection_gid).ToListAsync());
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public async Task<IActionResult> GetConnector(int id)
        {
            var connector = await dbContext.con_mst_tconnection.FindAsync(id);

            try
            {
                if (connector == null)

                {
                    return NotFound("Not Found");
                }
                return Ok(connector);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> AddConnector([FromBody] AddConnectorRequest addConnectorRequest)
        {
            try
            {
                int count = dbContext.con_mst_tconnection.Count();
                int maxId;
                if (count > 0)
                {
                    maxId = dbContext.con_mst_tconnection.Max(entity => entity.connection_gid);
                    maxId = maxId + 1;
                }
                else
                {
                    maxId = 1;
                }

                var connector = new ConnectionModel()
                {
                    connection_gid = 0,
                    connection_code = "CONN_" + ((maxId.ToString()).PadLeft(4, '0')),
                    connection_name = addConnectorRequest.connection_name,
                    connection_desc = addConnectorRequest.connection_desc,
                    source_db_type = addConnectorRequest.source_db_type,
                    protection_type = addConnectorRequest.protection_type,
                    file_password = addConnectorRequest.file_password,
                    source_host_name = addConnectorRequest.source_host_name,
                    source_port = addConnectorRequest.source_port,
                    source_auth_mode = addConnectorRequest.source_auth_mode,
                    source_db_user = addConnectorRequest.source_db_user,
                    source_db_pwd = addConnectorRequest.source_db_pwd,
                    source_auth_file_name = addConnectorRequest.source_auth_file_name,
                    source_auth_file_blob = addConnectorRequest.source_auth_file_blob,
                    having_auth_url = addConnectorRequest.having_auth_url,
                    source_file = addConnectorRequest.source_file,
                    ssh_tunneling = addConnectorRequest.ssh_tunneling,
                    ssh_host_name = addConnectorRequest.ssh_host_name,
                    ssh_port = addConnectorRequest.ssh_port,
                    ssh_user = addConnectorRequest.ssh_user,
                    ssh_pwd = addConnectorRequest.ssh_pwd,
                    ssh_auth_mode = addConnectorRequest.ssh_auth_mode,
                    ssh_file_name = addConnectorRequest.ssh_file_name,
                    ssh_file_blob = addConnectorRequest.ssh_file_blob,
                    connection_status = addConnectorRequest.connection_status,
                    created_date = addConnectorRequest.created_date,
                    created_by = addConnectorRequest.created_by,
                    updated_date = addConnectorRequest.updated_date,
                    updated_by = addConnectorRequest.updated_by,
                    delete_flag = "N"
                };
                await dbContext.con_mst_tconnection.AddAsync(connector);
                await dbContext.SaveChangesAsync();

                var lastInsertedId = connector.connection_gid;

                return Ok(lastInsertedId);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> UpdateConnector([FromBody] UpdateConnectorRequest updateConnectorRequest)
        {
            var connector = await dbContext.con_mst_tconnection.FindAsync(Convert.ToInt32(updateConnectorRequest.connection_gid));
            try
            {
                if (connector != null)
                {
                    //connector.connection_code = updateConnectorRequest.connection_code;
                    connector.connection_name = updateConnectorRequest.connection_name;
                    connector.connection_desc = updateConnectorRequest.connection_desc;
                    connector.source_db_type = updateConnectorRequest.source_db_type;
                    connector.protection_type = updateConnectorRequest.protection_type;
                    connector.file_password = updateConnectorRequest.file_password;
                    connector.source_host_name = updateConnectorRequest.source_host_name;
                    connector.source_port = updateConnectorRequest.source_port;
                    connector.source_auth_mode = updateConnectorRequest.source_auth_mode;
                    connector.source_db_user = updateConnectorRequest.source_db_user;
                    connector.source_db_pwd = updateConnectorRequest.source_db_pwd;
                    connector.source_auth_file_name = updateConnectorRequest.source_auth_file_name;
                    connector.source_auth_file_blob = updateConnectorRequest.source_auth_file_blob;
                    connector.having_auth_url = updateConnectorRequest.having_auth_url;
                    connector.source_file = updateConnectorRequest.source_file;
                    connector.ssh_tunneling = updateConnectorRequest.ssh_tunneling;
                    connector.ssh_host_name = updateConnectorRequest.ssh_host_name;
                    connector.ssh_port = updateConnectorRequest.ssh_port;
                    connector.ssh_user = updateConnectorRequest.ssh_user;
                    connector.ssh_pwd = updateConnectorRequest.ssh_pwd;
                    connector.ssh_auth_mode = updateConnectorRequest.ssh_auth_mode;
                    connector.ssh_file_name = updateConnectorRequest.ssh_file_name;
                    connector.ssh_file_blob = updateConnectorRequest.ssh_file_blob;
                    connector.connection_status = updateConnectorRequest.connection_status;
                    connector.updated_date = updateConnectorRequest.updated_date;
                    connector.updated_by = updateConnectorRequest.updated_by;
                    //connector.delete_flag = "Y";

                    await dbContext.SaveChangesAsync();

                    return Ok(connector.connection_gid);

                }
                return NotFound("Not Found TO Update");
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> DeleteConnector([FromBody] int id)
        {
            var connector = await dbContext.con_mst_tconnection.FindAsync(Convert.ToInt32(id));
            try
            {
                if (connector != null)
                {
                    connector.connection_status = "Inactive";
                    //connector.delete_flag = "Y";
                    await dbContext.SaveChangesAsync();
                    return Ok("Record Deleted Successfully");
                }

                return NotFound("Not Found TO Update");
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> Establishconnection([FromBody] AddConnectorRequest establishconn)
        {
            try
            {
                if (establishconn.source_db_type == "MySql")
                {
                    connectionString = "server=" + establishconn.source_host_name + "; uid=" + establishconn.source_db_user + "; pwd=" + establishconn.source_db_pwd + ";";
                    mysqlcnn = new MySqlConnection(connectionString);
                    mysqlcnn.Open();
                    mysqlcnn.Close();
                    return Ok("Test Connection Success..!");

                }
                else if (establishconn.source_db_type == "Postgres")
                {
                    connectionString = "Host=" + establishconn.source_host_name + "; Database=postgres" + "; Username=" + establishconn.source_db_user + "; Password=" + establishconn.source_db_pwd + ";";
                    postgrescnn = new NpgsqlConnection(connectionString);
                    postgrescnn.Open();
                    postgrescnn.Close();
                    return Ok("Test Connection Success..!");

                }
                else
                {
                    return Ok("Test Connection Failed..!");
                }


            }
            catch (Exception ex)
            {
                return Ok(ex.Message);
            }
        }
        [HttpGet]
        public string CreateSshTunnel(string sshHost, string sshUsername, string privateKeyFilePath, string remoteHost, int remotePort, string passphrase)
        {
            try
            {
                using (var privateKeyFile = new PrivateKeyFile(privateKeyFilePath, null))
                {
                    var keyFiles = new[] { privateKeyFile };
                    //var authenticationMethods = new List<AuthenticationMethod>();
                    //authenticationMethods.Add(new PrivateKeyAuthenticationMethod(sshUsername, keyFiles));
                    //authenticationMethods.Add(new PasswordAuthenticationMethod(sshUsername, "Flexi@123"));
                    var methods = new List<AuthenticationMethod>
                    {
                        new PasswordAuthenticationMethod(sshUsername, "Flexi@123"),
                        new PrivateKeyAuthenticationMethod(sshUsername, keyFiles)
                    };

                    var connectionInfo = new ConnectionInfo(sshHost, 22, sshUsername, methods.ToArray());

                    using (var client = new SshClient(connectionInfo))
                    {
                        client.Connect();

                        if (client.IsConnected)
                        {
                            var localPort = new ForwardedPortLocal("localhost", (uint)remotePort, remoteHost, (uint)remotePort);
                            client.AddForwardedPort(localPort);

                            localPort.Start();

                            string connectionString = $"Server=localhost;Port={localPort.BoundPort};Database=your_database;User Id={sshUsername};";
                        }
                        else
                        {
                            return "failed..!";
                        }
                    }
                    return "success..!";
                }
            }
            catch (Exception ex)
            {
                // Handle the exception and log the error message for debugging
                return ($"Error: {ex.Message}");
            }
        }

        [HttpPost]
        public async Task<IActionResult> AddapiauthToken([FromBody] AddapiauthTokenModel objAddapiauthToken)
        {
            try
            {
                int count = dbContext.con_mst_tpplapiauthtoken.Count();
                int maxId;
                if (count > 0)
                {
                    maxId = dbContext.con_mst_tpplapiauthtoken.Max(entity => entity.apiauthtoken_gid);
                    maxId = maxId + 1;
                }
                else
                {
                    maxId = 1;
                }

                var connector = new ApiAuthToken()
                {
                    apiauthtoken_gid = 0,
                    connection_code = objAddapiauthToken.connection_code,
                    auth_token_keyname = objAddapiauthToken.auth_token_keyname,
                    auth_url = objAddapiauthToken.auth_url,
                    auth_method = objAddapiauthToken.auth_method,
                    auth_token = objAddapiauthToken.auth_token,
                    auth_header_json = !string.IsNullOrEmpty(objAddapiauthToken.auth_header_json) ? objAddapiauthToken.auth_header_json : "[]",
                    auth_payload_type = objAddapiauthToken.auth_payload_type,
                    auth_body_format = objAddapiauthToken.auth_body_format,
                    auth_payload_json = !string.IsNullOrEmpty(objAddapiauthToken.auth_payload_json) ? objAddapiauthToken.auth_payload_json : "[]",
                    auth_response = objAddapiauthToken.auth_response,
                    remarks = objAddapiauthToken.remarks,
                    created_date = objAddapiauthToken.created_date,
                    created_by = objAddapiauthToken.created_by,
                    updated_date = objAddapiauthToken.updated_date,
                    updated_by = objAddapiauthToken.updated_by,
                    delete_flag = "N"
                };
                await dbContext.con_mst_tpplapiauthtoken.AddAsync(connector);
                await dbContext.SaveChangesAsync();

                var lastInsertedId = connector.apiauthtoken_gid;

                return Ok(lastInsertedId);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

        [HttpGet]
        public async Task<IActionResult> GetApiAuthToken(string connection_code)
        {
            var connector = await dbContext.con_mst_tpplapiauthtoken
                            .FirstOrDefaultAsync(x => x.connection_code == connection_code);
            try
            {
                if (connector == null)

                {
                    return NotFound("Not Found");
                }
                return Ok(connector);
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }


        [HttpPost] 
        public async Task<IActionResult> UpdateapiauthToken([FromBody] UpdateapiauthTokenModel objUpdateapiauthToken)
        {
            var apiauthtoken = await dbContext.con_mst_tpplapiauthtoken.FindAsync(Convert.ToInt32(objUpdateapiauthToken.apiauthtoken_gid));
            try
            {
                if (apiauthtoken != null)
                {
                    apiauthtoken.connection_code = objUpdateapiauthToken.connection_code;
                    apiauthtoken.auth_token_keyname = objUpdateapiauthToken.auth_token_keyname;
                    apiauthtoken.auth_url = objUpdateapiauthToken.auth_url;
                    apiauthtoken.auth_method = objUpdateapiauthToken.auth_method;
                    apiauthtoken.auth_token = objUpdateapiauthToken.auth_token;
                    apiauthtoken.auth_header_json = !string.IsNullOrEmpty(objUpdateapiauthToken.auth_header_json) ? objUpdateapiauthToken.auth_header_json : "[]";
                    apiauthtoken.auth_payload_type = objUpdateapiauthToken.auth_payload_type;
                    apiauthtoken.auth_body_format = objUpdateapiauthToken.auth_body_format;
                    apiauthtoken.auth_payload_json = !string.IsNullOrEmpty(objUpdateapiauthToken.auth_payload_json) ? objUpdateapiauthToken.auth_payload_json : "[]";
                    apiauthtoken.auth_response = objUpdateapiauthToken.auth_response;
                    apiauthtoken.remarks = objUpdateapiauthToken.remarks;
                    apiauthtoken.created_date = objUpdateapiauthToken.created_date;
                    apiauthtoken.created_by = objUpdateapiauthToken.created_by;
                    apiauthtoken.updated_date = objUpdateapiauthToken.updated_date;
                    apiauthtoken.updated_by = objUpdateapiauthToken.updated_by;
                    apiauthtoken.delete_flag = "N";
                   
                    await dbContext.SaveChangesAsync();

                    return Ok(apiauthtoken.apiauthtoken_gid);

                }
                return NotFound("Not Found TO Update");
            }
            catch (Exception ex)
            {
                return BadRequest($"Error: {ex.Message}");
            }
        }

    }
}
