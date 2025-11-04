using System;
using Microsoft.EntityFrameworkCore.Migrations;
using MySql.EntityFrameworkCore.Metadata;

#nullable disable

namespace MysqlEfCoreDemo.Migrations
{
    /// <inheritdoc />
    public partial class Initial : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterDatabase()
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_col_datatype",
                columns: table => new
                {
                    pipeline_code = table.Column<string>(type: "longtext", nullable: true),
                    dataset_code = table.Column<string>(type: "longtext", nullable: true),
                    dataset_field_type = table.Column<string>(type: "longtext", nullable: true),
                    sourcefield_name = table.Column<string>(type: "longtext", nullable: true),
                    dataset_field_name = table.Column<string>(type: "longtext", nullable: true),
                    source_type = table.Column<string>(type: "longtext", nullable: true)
                },
                constraints: table =>
                {
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_mst_tconfig",
                columns: table => new
                {
                    config_name = table.Column<string>(type: "longtext", nullable: true),
                    config_value = table.Column<string>(type: "longtext", nullable: true),
                    delete_flag = table.Column<string>(type: "longtext", nullable: true)
                },
                constraints: table =>
                {
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_mst_tconnection",
                columns: table => new
                {
                    connection_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    connection_code = table.Column<string>(type: "varchar(64)", nullable: true),
                    connection_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    connection_desc = table.Column<string>(type: "text", nullable: true),
                    source_db_type = table.Column<string>(type: "varchar(16)", nullable: true),
                    protection_type = table.Column<string>(type: "char(1)", nullable: true),
                    file_password = table.Column<string>(type: "varchar(64)", nullable: true),
                    source_host_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    source_port = table.Column<string>(type: "varchar(8)", nullable: true),
                    source_auth_mode = table.Column<string>(type: "varchar(32)", nullable: true),
                    source_db_user = table.Column<string>(type: "varchar(128)", nullable: true),
                    source_db_pwd = table.Column<string>(type: "varchar(64)", nullable: true),
                    source_auth_file_name = table.Column<string>(type: "text", nullable: true),
                    source_auth_file_blob = table.Column<byte[]>(type: "blob", nullable: true),
                    source_file = table.Column<string>(type: "text", nullable: true),
                    ssh_tunneling = table.Column<string>(type: "char(1)", nullable: true),
                    ssh_host_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    ssh_port = table.Column<string>(type: "varchar(8)", nullable: true),
                    ssh_user = table.Column<string>(type: "varchar(128)", nullable: true),
                    ssh_pwd = table.Column<string>(type: "varchar(64)", nullable: true),
                    ssh_auth_mode = table.Column<string>(type: "varchar(128)", nullable: true),
                    ssh_file_name = table.Column<string>(type: "text", nullable: true),
                    ssh_file_blob = table.Column<byte[]>(type: "blob", nullable: true),
                    connection_status = table.Column<string>(type: "varchar(64)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_mst_tconnection", x => x.connection_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_mst_tdataprocessing",
                columns: table => new
                {
                    dataprocessing_gid = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    dataprocessing_header_gid = table.Column<int>(type: "int(11)", nullable: false),
                    dataprocessing_child_master_gid = table.Column<long>(type: "bigint", nullable: false),
                    dataprocessing_parent_master_gid = table.Column<long>(type: "bigint", nullable: false),
                    dataprocessing_master_gid = table.Column<long>(type: "bigint", nullable: false),
                    dataprocessing_pipeline_gid = table.Column<int>(type: "int(11)", nullable: false),
                    dataprocessing_pplfieldmapping_gid = table.Column<int>(type: "int(11)", nullable: false),
                    dataprocessing_orderby = table.Column<int>(type: "int(11)", nullable: false),
                    dataprocessing_param1 = table.Column<string>(type: "varchar(50)", nullable: false),
                    dataprocessing_param2 = table.Column<string>(type: "varchar(50)", nullable: true),
                    dataprocessing_param3 = table.Column<string>(type: "varchar(50)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(50)", nullable: false),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(50)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_mst_tdataprocessing", x => x.dataprocessing_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_mst_tdataprocessingheader",
                columns: table => new
                {
                    dataprocessingheader_gid = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    dataprocessingheader_pipeline_code = table.Column<string>(type: "varchar(50)", nullable: false),
                    dataprocessingheader_pplfieldmapping_gid = table.Column<long>(type: "bigint(20)", nullable: false),
                    dataprocessingheader_seqno = table.Column<int>(type: "int(11)", nullable: false),
                    dataprocessingheader_ppl_field_name = table.Column<string>(type: "varchar(128)", nullable: false),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(50)", nullable: false),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(50)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_mst_tdataprocessingheader", x => x.dataprocessingheader_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_mst_tdataset",
                columns: table => new
                {
                    dataset_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    dataset_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    dataset_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    module_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    table_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    connector_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_mst_tdataset", x => x.dataset_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_mst_tdataset_field",
                columns: table => new
                {
                    dataset_field_gid = table.Column<int>(type: "int", nullable: false),
                    dataset_code = table.Column<string>(type: "longtext", nullable: true),
                    dataset_field_desc = table.Column<string>(type: "longtext", nullable: true),
                    dataset_field_name = table.Column<string>(type: "longtext", nullable: true),
                    dataset_field_type = table.Column<string>(type: "longtext", nullable: true),
                    dataset_table_field = table.Column<string>(type: "longtext", nullable: true),
                    field_mandatory = table.Column<string>(type: "longtext", nullable: true),
                    active_status = table.Column<string>(type: "longtext", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "longtext", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    updated_by = table.Column<string>(type: "longtext", nullable: true),
                    delete_flag = table.Column<string>(type: "longtext", nullable: true)
                },
                constraints: table =>
                {
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_mst_tfieldconfig",
                columns: table => new
                {
                    fieldconfig_gid = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    fieldconfig_master_code = table.Column<string>(type: "varchar(50)", nullable: false),
                    fieldconfig_expressions = table.Column<string>(type: "char(1)", nullable: false),
                    fieldconfig_expression_label = table.Column<string>(type: "varchar(150)", nullable: true),
                    fieldconfig_dynamicfields = table.Column<string>(type: "JSON", nullable: false),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(50)", nullable: false),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(50)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_mst_tfieldconfig", x => x.fieldconfig_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_mst_tmaster",
                columns: table => new
                {
                    master_gid = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    master_code = table.Column<string>(type: "varchar(50)", nullable: false),
                    master_name = table.Column<string>(type: "varchar(255)", nullable: false),
                    parent_gid = table.Column<long>(type: "bigint(20)", nullable: false),
                    parent_code = table.Column<string>(type: "varchar(50)", nullable: true),
                    depend_gid = table.Column<long>(type: "bigint(20)", nullable: false),
                    depend_code = table.Column<string>(type: "varchar(50)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: false),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_mst_tmaster", x => x.master_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_mst_tpipeline",
                columns: table => new
                {
                    pipeline_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    pipeline_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    pipeline_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    pipeline_desc = table.Column<string>(type: "text", nullable: true),
                    connection_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    db_name = table.Column<string>(type: "varchar(256)", nullable: true),
                    source_file_name = table.Column<string>(type: "varchar(256)", nullable: true),
                    sheet_name = table.Column<string>(type: "varchar(256)", nullable: true),
                    table_view_query_type = table.Column<string>(type: "char(1)", nullable: true),
                    table_view_query_desc = table.Column<string>(type: "text", nullable: true),
                    target_dataset_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    pipeline_status = table.Column<string>(type: "varchar(64)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_mst_tpipeline", x => x.pipeline_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_mst_tqcdmaster",
                columns: table => new
                {
                    master_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    master_syscode = table.Column<string>(type: "varchar(32)", nullable: true),
                    master_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    master_short_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    master_name = table.Column<string>(type: "varchar(255)", nullable: true),
                    master_multiple_name = table.Column<string>(type: "text", nullable: true),
                    parent_master_syscode = table.Column<string>(type: "varchar(32)", nullable: true),
                    depend_flag = table.Column<string>(type: "char(1)", nullable: true),
                    depend_master_syscode = table.Column<string>(type: "varchar(32)", nullable: true),
                    depend_parent_master_syscode = table.Column<string>(type: "varchar(32)", nullable: true),
                    system_flag = table.Column<string>(type: "char(1)", nullable: true),
                    active_status = table.Column<string>(type: "char(1)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_mst_tqcdmaster", x => x.master_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_trn_tapi",
                columns: table => new
                {
                    api_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    pipeline_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    api_jsonvalue = table.Column<string>(type: "TEXT", nullable: true),
                    api_key = table.Column<string>(type: "TEXT", nullable: true),
                    api_value = table.Column<string>(type: "TEXT", nullable: true),
                    remarks = table.Column<string>(type: "TEXT", nullable: true),
                    created_date = table.Column<string>(type: "varchar(32)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_trn_tapi", x => x.api_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_trn_tdatasetimport",
                columns: table => new
                {
                    dataset_import_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    pipelinecode = table.Column<string>(type: "varchar(32)", nullable: true),
                    dataset_info = table.Column<string>(type: "text", nullable: true),
                    job_gid = table.Column<int>(type: "int(11)", nullable: false),
                    status_code = table.Column<string>(type: "varchar(64)", nullable: true),
                    remarks = table.Column<string>(type: "text", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_trn_tdatasetimport", x => x.dataset_import_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_trn_tincrementalrecord",
                columns: table => new
                {
                    incremental_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    pipeline_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    incremental_field = table.Column<string>(type: "varchar(64)", nullable: true),
                    incremental_value = table.Column<string>(type: "varchar(64)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_trn_tincrementalrecord", x => x.incremental_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_trn_tpplcondition",
                columns: table => new
                {
                    pplcondition_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    pipeline_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    condition_type = table.Column<string>(type: "varchar(32)", nullable: true),
                    condition_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    condition_text = table.Column<string>(type: "text", nullable: true),
                    condition_msg = table.Column<string>(type: "text", nullable: true),
                    sys_flag = table.Column<string>(type: "char(1)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_trn_tpplcondition", x => x.pplcondition_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_trn_tpplcsvheader",
                columns: table => new
                {
                    pplcsvheader_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    pipeline_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    column_separator = table.Column<string>(type: "varchar(128)", nullable: true),
                    number_ofcolumns = table.Column<int>(type: "int(11)", nullable: false),
                    number_oflines_toskip = table.Column<int>(type: "int(11)", nullable: false),
                    csvfile_dateformat = table.Column<string>(type: "varchar(128)", nullable: true),
                    csvfile_datetimeformat = table.Column<string>(type: "varchar(128)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_trn_tpplcsvheader", x => x.pplcsvheader_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_trn_tpplfieldmapping",
                columns: table => new
                {
                    pplfieldmapping_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    pipeline_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    dataset_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    pplfieldmapping_flag = table.Column<int>(type: "boolean", nullable: false),
                    ppl_field_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    dataset_field_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    default_value = table.Column<string>(type: "varchar(128)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_trn_tpplfieldmapping", x => x.pplfieldmapping_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_trn_tpplfinalization",
                columns: table => new
                {
                    finalization_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    pipeline_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    run_type = table.Column<string>(type: "varchar(64)", nullable: true),
                    cron_expression = table.Column<string>(type: "varchar(64)", nullable: true),
                    extract_mode = table.Column<string>(type: "varchar(64)", nullable: true),
                    upload_mode = table.Column<string>(type: "varchar(64)", nullable: true),
                    key_field = table.Column<string>(type: "text", nullable: true),
                    extract_condition = table.Column<string>(type: "text", nullable: true),
                    pull_days = table.Column<int>(type: "int", nullable: false),
                    reject_duplicate_flag = table.Column<string>(type: "char(1)", nullable: true),
                    error_mode = table.Column<string>(type: "varchar(32)", nullable: true),
                    last_incremental_val = table.Column<string>(type: "varchar(255)", nullable: true),
                    parent_dataset_code = table.Column<string>(type: "varchar(255)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_trn_tpplfinalization", x => x.finalization_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_trn_tpplsourcefield",
                columns: table => new
                {
                    pplsourcefield_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    pipeline_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    sourcefield_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    sourcefield_sno = table.Column<int>(type: "int(11)", nullable: false),
                    sourcefield_datatype = table.Column<string>(type: "varchar(128)", nullable: true),
                    sourcefield_expression = table.Column<string>(type: "text", nullable: true),
                    source_type = table.Column<string>(type: "varchar(32)", nullable: true),
                    dataset_table_field = table.Column<string>(type: "varchar(128)", nullable: true),
                    dataset_table_field_sno = table.Column<int>(type: "int(11)", nullable: false),
                    cast_dataset_table_field = table.Column<string>(type: "text", nullable: true),
                    expressionfield_json = table.Column<string>(type: "json", nullable: true),
                    sourcefieldmapping_flag = table.Column<string>(type: "char(1)", nullable: true),
                    created_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    created_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    updated_date = table.Column<DateTime>(type: "datetime(6)", nullable: true),
                    updated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_trn_tpplsourcefield", x => x.pplsourcefield_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "con_trn_tscheduler",
                columns: table => new
                {
                    scheduler_gid = table.Column<int>(type: "int", nullable: false)
                        .Annotation("MySQL:ValueGenerationStrategy", MySQLValueGenerationStrategy.IdentityColumn),
                    scheduled_date = table.Column<DateTime>(type: "datetime", nullable: false),
                    pipeline_code = table.Column<string>(type: "varchar(32)", nullable: true),
                    scheduler_parameters = table.Column<string>(type: "text", nullable: true),
                    file_path = table.Column<string>(type: "text", nullable: true),
                    file_name = table.Column<string>(type: "varchar(128)", nullable: true),
                    scheduler_start_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    scheduler_end_date = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    scheduler_status = table.Column<string>(type: "varchar(32)", nullable: true),
                    additional_status = table.Column<string>(type: "varchar(32)", nullable: true),
                    scheduler_remark = table.Column<string>(type: "text", nullable: true),
                    scheduler_initiated_by = table.Column<string>(type: "varchar(32)", nullable: true),
                    last_update_date = table.Column<DateTime>(type: "datetime", nullable: false),
                    delete_flag = table.Column<string>(type: "char(1)", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_con_trn_tscheduler", x => x.scheduler_gid);
                })
                .Annotation("MySQL:Charset", "utf8mb4");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "con_col_datatype");

            migrationBuilder.DropTable(
                name: "con_mst_tconfig");

            migrationBuilder.DropTable(
                name: "con_mst_tconnection");

            migrationBuilder.DropTable(
                name: "con_mst_tdataprocessing");

            migrationBuilder.DropTable(
                name: "con_mst_tdataprocessingheader");

            migrationBuilder.DropTable(
                name: "con_mst_tdataset");

            migrationBuilder.DropTable(
                name: "con_mst_tdataset_field");

            migrationBuilder.DropTable(
                name: "con_mst_tfieldconfig");

            migrationBuilder.DropTable(
                name: "con_mst_tmaster");

            migrationBuilder.DropTable(
                name: "con_mst_tpipeline");

            migrationBuilder.DropTable(
                name: "con_mst_tqcdmaster");

            migrationBuilder.DropTable(
                name: "con_trn_tapi");

            migrationBuilder.DropTable(
                name: "con_trn_tdatasetimport");

            migrationBuilder.DropTable(
                name: "con_trn_tincrementalrecord");

            migrationBuilder.DropTable(
                name: "con_trn_tpplcondition");

            migrationBuilder.DropTable(
                name: "con_trn_tpplcsvheader");

            migrationBuilder.DropTable(
                name: "con_trn_tpplfieldmapping");

            migrationBuilder.DropTable(
                name: "con_trn_tpplfinalization");

            migrationBuilder.DropTable(
                name: "con_trn_tpplsourcefield");

            migrationBuilder.DropTable(
                name: "con_trn_tscheduler");
        }
    }
}
