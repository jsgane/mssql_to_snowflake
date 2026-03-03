# Define SQL Server connection and file details
$SqlServer      = "bodsql\app01"
$SqlDBName      = "comon_prod"
$SqlUsername    = "sqlwindev"
$SqlPassword    = "sql1234*"
$TableName      = "dbo.VLinkLocalisation"
$OutputPath     = "C:\Temp\mssql_vlnk_exported_data.csv"
$Delimiter      = ","
$Query          = "SELECT top 10000000 * FROM $TableName with (nolock)"

$StageName      = "MSSQL_DIRECT_STAGE"
$FileFormatName = "mssql_csv_file_format"

# Define Snowflake connection details
$SnowflakeAccount   = "dg63583.eu-west-1" #"webhdwh-rj60778"
$SnowflakeUser      = "neemba_user"
$SnowflakeRole      = "transform"
$SnowflakePassword  = "Neemb@Password2025"
$SnowflakeWarehouse = "compute_wh"
$SnowflakeDatabase  = "NEEMBA"
$SnowflakeSchema    = "EQUIPEMENT"
$SnowflakeTable     = "a_bronze_vlinklocalisation" # 


# SQL creat file format on snowflake
$sql_create_file_format = @"
CREATE OR REPLACE FILE FORMAT $FileFormatName
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', '')
    EMPTY_FIELD_AS_NULL = TRUE;
"@
$sql_create_file_format_file = "C:\Temp\sql_create_file_format.sql"
[System.IO.File]::WriteAllText(
    $sql_create_file_format_file,
    $sql_create_file_format,
    (New-Object System.Text.UTF8Encoding($false))
)

# SQL creat stage on snowflake
$sql_create_stage = @"
CREATE OR REPLACE STAGE $StageName
    FILE_FORMAT = $FileFormatName;
"@

$sql_create_stage_file = "C:\Temp\create_stage.sql"
[System.IO.File]::WriteAllText(
    $sql_create_stage_file,
    $sql_create_stage,
    (New-Object System.Text.UTF8Encoding($false))
)

$sql_create_table = @"
create or alter table neemba.equipement.a_bronze_vlinklocalisation(
	idvlinklocalisation bigint null,
	codeconstructeursource varchar(4) null,
	numeroseriesource varchar(50) null,
	dateheurecreation timestamp_ntz(7) null,
	dateheuredonnee timestamp_ntz(7) null,
	latitude float null,
	longitude float null,
	validity varchar(10) null,
	servicemeter_value float null,
	servicemeter_uom varchar(20) null,
	modulecode varchar(20) null,
	moduletime timestamp_ntz(7) null,
	receivedtime timestamp_ntz(7) null,
	idmessage int null,
	idmastermessage bigint null,
	sourcedonnee varchar(50) null,
	dateheureintegration datetime null,
	idvlinkentete bigint null
);

"@

$sql_create_table_file = "C:\Temp\sql_create_table.sql"
[System.IO.File]::WriteAllText(
    $sql_create_table_file,
    $sql_create_table,
    (New-Object System.Text.UTF8Encoding($false))
)

# Use bcp to export data to a CSV file
# -c specifies character data type (for CSV)
# -t specifies the field terminator
# -S specifies the server
# -d specifies the database
# -T specifies a trusted connection (Windows authentication)

New-Item -ItemType Directory -Force -Path (Split-Path $OutputPath) | Out-Null ### create the target directory

# Remove the file if it exists
if (Test-Path $OutputPath) {
    Remove-Item $OutputPath -Force
    Write-Host "Removed existing file: $OutputPath"
}

$bcpExe = "C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe"

Write-Host "Executing bcp command:"

& $bcpExe "`"$Query`"" queryout "`"$OutputPath`"" -c -t "," -S $SqlServer -d $SqlDBName -U $SqlUsername -P $SqlPassword
Write-Host "Data exported to $OutputPath" -ForegroundColor Green

###### To use parquet do ##############################################
### Write-Host "Conversion CSV → Parquet..."                        ### 
### python - <<EOF                                                  ### 
### import pandas as pd                                             ### 
### df = pd.read_csv(r'$csvPath')                                   ### 
### df.to_parquet(r'$parquetPath', engine='pyarrow', index=False)   ### 
### EOF                                                             ### 
### Create a parquet file format and a stage using this file format ###
### Use this stage to perform the put command                       ###
#######################################################################

# Create the snowflake file format
$snowsqlArgs = @(
    "-a", $SnowflakeAccount,
    "-u", $SnowflakeUser,
    "-w", $SnowflakeWarehouse,
    "-d", $SnowflakeDatabase,
    "-s", $SnowflakeSchema,
    "-r", $SnowflakeRole,
    "-f", $sql_create_file_format_file
)

try {
      & snowsql @snowsqlArgs
      Write-Host "✅ File format $FileFormatName created" -ForegroundColor Green
}
catch {
    Write-Host "❌ Erreur creating the File format: $_" -ForegroundColor Red
    throw
}

# Create the snowflake stage
$snowsqlArgs = @(
    "-a", $SnowflakeAccount,
    "-u", $SnowflakeUser,
    "-w", $SnowflakeWarehouse,
    "-d", $SnowflakeDatabase,
    "-s", $SnowflakeSchema,
    "-r", $SnowflakeRole,
    "-f", $sql_create_stage_file
)

try {
      & snowsql @snowsqlArgs
      Write-Host "✅ Stage $StageName created with a csv format" -ForegroundColor Green
}
catch {
    Write-Host "❌ Erreur creating the stage: $_" -ForegroundColor Red
    throw
}

## Create the target table
$snowsqlArgs = @(
    "-a", $SnowflakeAccount,
    "-u", $SnowflakeUser,
    "-w", $SnowflakeWarehouse,
    "-d", $SnowflakeDatabase,
    "-s", $SnowflakeSchema,
    "-f", $sql_create_table_file
)
try {
      & snowsql @snowsqlArgs
      Write-Host "✅ The table created successfully" -ForegroundColor Green
}
catch {
    Write-Host "❌ Error creating the table: $_" -ForegroundColor Red
    throw
}

# Command to upload the file to a Snowflake internal stage using the PUT command
$sql_put = "PUT file://$OutputPath @$SnowflakeStage AUTO_COMPRESS=TRUE"
$snowsqlArgs = @(
    "-a", $SnowflakeAccount,
    "-u", $SnowflakeUser,
    "-w", $SnowflakeWarehouse,
    "-d", $SnowflakeDatabase,
    "-s", $SnowflakeSchema,
    "-q", $sql_put
)
try {
     Write-Host "Executing Snowflake PUT command..."  
     & snowsql @snowsqlArgs
     Write-Host "✅ Data put to $StageName" -ForegroundColor Green
}
catch {
    Write-Host "❌ Error executing the put command: $_" -ForegroundColor Red
    throw
}


# Command to copy data from the stage into the target table using the COPY INTO command

$sql_copy_into = @"
COPY INTO $SnowflakeTable
FROM @$StageName
FILE_FORMAT = (FORMAT_NAME = $FileFormatName)
ON_ERROR = 'ABORT_STATEMENT';
"@

$snowsqlArgs = @(
    "-a", $SnowflakeAccount,
    "-u", $SnowflakeUser,
    "-w", $SnowflakeWarehouse,
    "-d", $SnowflakeDatabase,
    "-s", $SnowflakeSchema,
    "-q", $sql_copy_into
)
try {
     Write-Host "Executing Snowflake COPY INTO command..."
     & snowsql @snowsqlArgs
     Write-Host "✅ Data insert to the target table $SnowflakeTable." -ForegroundColor Green
}
catch {
    Write-Host "❌ Error executing the put command: $_" -ForegroundColor Red
    throw
}



