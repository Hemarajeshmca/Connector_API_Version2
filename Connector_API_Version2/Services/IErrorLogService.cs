using Microsoft.AspNetCore.Mvc;
using MysqlEfCoreDemo.Models;
using System.Threading.Tasks;

namespace MysqlEfCoreDemo.Services
{
    public interface IErrorLogService
    {
        Task LogErrorAsync(ErrorLog objerrorlog);
    }
}
