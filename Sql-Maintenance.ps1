<#
    .SYNOPSIS
    Perform common maintenance tasks like DBCC CHECKDB or rebuild indexes on SQL Server
    .DESCRIPTION
    This script can perform several maintenance tasks on SQL Server.

    The tasks it can perform are conditionally rebuilding or reorganizing indexes when needed. Statistics that need an update can be updated. It can also take care of consistency checks and database backups.
    .EXAMPLE
    Sql-Maintenance -SqlServer CONTOSO1 -UpdateStatistics -RebuildIndexes -CheckDatabases -BackupDatabases -BackupPath C:\backup -BackupType Full
    Do everything on a single server using default settings.
    .EXAMPLE
    Sql-Maintenance -SqlServer CONTOSO1 -UpdateStatistics
    Update statistics on a single server.
    .EXAMPLE
    Sql-Maintenance -SqlServer CONTOSO1 -BackupDatabases -BackupPath C:\backup -BackupType Log
    Create a log backup on a group of servers.
    .EXAMPLE
    Sql-Maintenance -SqlServer CONTOSO1 -RebuildIndexes -LowWaterMark 50 -HighWaterMark 80 -DoNotRebuildOnline
    Reorganize indexes when fragmentation is above 50 and rebuild indexes when fragmentation is above 80 on a single server. Never rebuild online.
    .PARAMETER SqlServer
    The name of the single server to perform activities on. Valid options are {ServerName}, {ServerName\InstanceName} or {ServerName,PortNumber}
    .PARAMETER SqlCredential
    A PSCredential that stores the user information when SQL authentication is used. For Windows authentication this parameter can be omitted.
    .PARAMETER Include
    An array of databasenames that are included in the tasks. Other databases are skipped. System databases can be added with the alias 'system'. When exclude is specified include has no effect.
    .PARAMETER Exclude
    An array of databasenames that are excluded in the tasks. System databases can be added with the alias 'system'.
    .PARAMETER UpdateStatistics
    A switch to determine whether or not to update statistics
    .PARAMETER RebuildIndexes
    A switch to determine whether or not to rebuild or reorganize indexes
    .PARAMETER DoNotRebuildOnline
    A switch to tell the script to never rebuild online. The default is to rebuild indexes online when possible. When this switch is provided index rebuilds will always be performed offline. Index reorganizations are always performed online.
    .PARAMETER LowWaterMark
    A number that determines the level of fragmentation before an index reorganization is considered.
    .PARAMETER HighWaterMark
    A number that determines the level of fragementation before an index rebuild is considered.
    .PARAMETER BackupDatabases
    A switch to determine whether or not to backup databases.
    .PARAMETER BackupPath
    The path where backups are stored. If the directory doesn't exist it will be created. All backups are created according to the following structure:
    {BackupPath}\{ServerName|ServerName\InstanceName|AvailabilityGroupName}\{DatabaseName}\{DatabaseName}_{BackupType}_{TimeStamp}.bak
    .PARAMETER BackupType
    The backuptype that has to be created. Valid options are Full, Differential or Log. When a database has not been backed up yet a full backup will be taken on the primary database. When the database is part of an availability group and the backup preference is for a secondary a copy only backup will be performed.
#>
[CmdletBinding()]
Param (
    [string]$SqlServer = $env:COMPUTERNAME,
    [string]$LogPath,
    [pscredential]$SqlCredential,
    [string[]]$Exclude,
    [string[]]$Include,
    [switch]$UpdateStatistics,
    [ValidateRange(0, 100)]
    [int]$SamplePercent = 100,
    [switch]$RebuildIndexes,
    [switch]$DoNotRebuildOnline,
    [ValidateRange(0, 100)]
    [int]$LowWaterMark = 10,
    [ValidateRange(0, 100)]
    [int]$HighWaterMark = 30,
    [switch]$CheckDatabases,
    [switch]$BackupDatabases,
    [string]$BackupPath,
    [ValidateSet("Full", "Log", "Differential")]
    $BackupType = "Full"
)
#region Internal Constants
[string]$ERROR_SERVER_INFORMATION = "Failed to retrieve server information. The process is exiting."
[string]$ERROR_DATABASE_BACKUP = "There was an error creating the backup. Review the errorlog for more information."
[string]$ERROR_DATABASE_CHECKDB = "There was an error running DBCC CHECKDB. Review the errorlog for more information."
#endregion

#region Internal Functions
function ExecuteSqlQuery {
    Param (
        [string]$connectionString,
        [string]$query
    )

    $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
    if ($SqlCredential -ne $null) {
        #$Login = New-Object System.Management.Automation.PSCredential -ArgumentList $SqlUser, $Password
        $SqlCredential.Password.MakeReadOnly()
        $sqlCred = New-Object System.Data.SqlClient.SqlCredential($SqlCredential.UserName, $SqlCredential.Password)
        $connection.Credential = $sqlCred
    }
    $command = New-Object System.Data.SqlClient.SqlCommand($query, $connection)
    $command.CommandTimeout = 0
    $datatable = New-Object System.Data.DataTable
    try {
        $connection.Open()
        $datatable.Load($command.ExecuteReader())
    }
    catch {
        throw $_.Exception
    }
    finally {
        $connection.Dispose()
    }
    return $datatable
}

Function WriteLog {
    Param(
        [ValidateSet("INFO", "WARN", "ERRO", "FATL", "DEBG", "CODE")]
        [string]$Level = "INFO",
        [Parameter(Mandatory = $True)]
        [string]$Message
    )

    $stamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $line = "$Stamp $Level $RUN_UID $Message"
    Add-Content $LogFile -Value $line
    switch ($Level) {
        "WARN" { Write-Warning $line}
        "ERRO" { Write-Error $line; $Script:ErrorCount++ }
        "FATL" { Write-Error $line; $Script:ErrorCount++ }
        "DEBG" { Write-Verbose $line}
        "INFO" { Write-Host $line}
        "CODE" { Write-Verbose $line}
    }
}
function GetServerDetails {
    Param (
        [string]$connectionString,
        [int]$version = 99
    )

    [string]$QUERY_VERSION = "SELECT SERVERPROPERTY('ProductVersion') AS ProductVersion, SERVERPROPERTY('Edition') AS Edition;";
    $result = ExecuteSqlQuery -connectionString $connectionString -query $QUERY_VERSION
    return $result
}

function GetDatabases {
    Param (
        [string]$connectionString,
        [switch]$system,
        [int]$majorversion = 99,
        [bool]$azure = $false
    )
    [string]$QUERY_DATABASES = "SELECT 
	dbs.database_id, 
	dbs.name as database_name, 
	dbs.compatibility_level, 
	dbs.user_access, 
	dbs.is_read_only, 
	dbs.recovery_model, 
	dbs.recovery_model_desc,
	dbrs.database_guid, 
	dbrs.last_log_backup_lsn,
	ISNULL((SELECT TOP 1 cluster_name FROM sys.dm_hadr_cluster) + '\' + ag.name, @@SERVERNAME) AS path_name, 
	ISNULL(ags.primary_replica, @@SERVERNAME) AS primary_replica, 
	ISNULL(ag.automated_backup_preference, 0) AS automated_backup_preference,
	CASE WHEN ISNULL(ags.primary_replica, @@SERVERNAME) = @@SERVERNAME THEN 1 ELSE 0 END AS is_primary,
	sys.fn_hadr_backup_is_preferred_replica(dbs.name) AS preferred_replica,
	ISNULL(drs.synchronization_health, 255) AS synchronization_health,
	@@SERVERNAME AS local_server_name
FROM sys.databases dbs
INNER JOIN sys.database_recovery_status dbrs ON dbs.database_id = dbrs.database_id 
LEFT JOIN sys.dm_hadr_database_replica_states drs ON dbs.group_database_id = drs.group_database_id AND drs.is_local = 1
LEFT JOIN sys.availability_groups ag ON drs.group_id = ag.group_id
LEFT JOIN sys.dm_hadr_availability_group_states ags ON drs.group_id = ags.group_id
WHERE dbs.state = 0;"

    if ($azure) {
        $QUERY_DATABASES = "SELECT dbs.database_id, dbs.name as database_name, dbs.compatibility_level, dbs.user_access, dbs.is_read_only, dbs.recovery_model, dbs.recovery_model_desc,
NEWID() AS database_guid,0 AS last_log_backup_lsn,
@@SERVERNAME AS path_name, @@SERVERNAME AS primary_replica, 0 AS automated_backup_preference,
1 AS is_primary,
1 AS preferred_replica,
255 AS synchronization_health,
@@SERVERNAME AS local_server_name 
FROM sys.databases dbs"
    }
    return ExecuteSqlQuery -connectionString $connectionString -query $QUERY_DATABASES
}

function GetIndexes {
    Param(
        [string]$connectionString
    )
    $QUERY_INDEXES = "SELECT 
	s.name AS schema_name, 
	o.name AS object_name, 
	i.name AS index_name, 
	ips.index_id, 
	i.type AS index_type,
	i.type_desc AS index_type_desc,
	ISNULL(ironci.cpoo, 1) AS nci_rebuilt_online,
	ISNULL(iroci.cpoo, 1) ci_rebuilt_online,
	ips.partition_number, 
	ips.avg_fragmentation_in_percent ,
	(SELECT COUNT(*) FROM sys.partitions p WHERE p.object_id = ips.object_id and p.index_id = i.index_id) AS partition_count
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
INNER JOIN sys.objects o ON ips.object_id = o.object_id
INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
LEFT JOIN (SELECT ic.object_id, ic.index_id, 0 AS cpoo FROM sys.index_columns ic 
				INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
				WHERE c.user_type_id IN (34, 35,99)
				GROUP BY ic.object_id, ic.index_id) ironci ON ironci.index_id = i.index_id AND ironci.object_id = i.object_id
LEFT JOIN (SELECT object_id, 0 AS cpoo FROM sys.columns c 
				WHERE c.user_type_id IN (34,35,99)
				GROUP BY object_id) iroci ON iroci.object_id = i.object_id
WHERE o.is_ms_shipped = 0 AND i.index_id > 0 AND i.is_disabled = 0;"

    return ExecuteSqlQuery -connectionString $connectionString -query $QUERY_INDEXES
}

function GetStatistics {
    Param(
        [string]$connectionString
    )
    $QUERY_STATS = "SELECT 
	s.name AS [schema_name], 
	o.name AS object_name,
	st.name AS [stats_name], 
	ISNULL(dsp.rows, -1) AS [stats_rows], 
	ISNULL(modification_counter, -1) AS [stats_modification_counter],
	dsp.rows_sampled,
	dsp.last_updated,
	sum(reserved_page_count*8.0)/1024 AS [table_size],
	SUM(ps.row_count) AS row_count
FROM sys.stats st
INNER JOIN sys.objects o on st.object_id = o.object_id
INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
INNER JOIN sys.dm_db_partition_stats AS ps on o.object_id = ps.object_id
CROSS APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) dsp
WHERE 
	o.is_ms_shipped = 0
GROUP BY 
	s.name, o.name, st.name, ISNULL(dsp.rows, -1), 
	ISNULL(modification_counter, -1), dsp.rows_sampled,	dsp.last_updated;"

    return ExecuteSqlQuery -connectionString $connectionString -query $QUERY_STATS
}

#endregion

function IndexMaintenance {
    Param(
        [string]$connectionString,
        [int]$lowthreshold,
        [int]$highthreshold
    )
    $indexes = GetIndexes -connectionString $connectionString
    foreach ($index in $indexes) {
        [bool]$rebuild = $false
        $QUERY_REORGINDEX = "ALTER INDEX [$($index.index_name)] ON [$($index.schema_name)].[$($index.object_name)]"
        if ($index.avg_fragmentation_in_percent -gt $lowthreshold) {
            if ($index.avg_fragmentation_in_percent -lt $highthreshold) {
                WriteLog -Level INFO -Message "Not hitting the threshold... $($index.avg_fragmentation_in_percent) $($highthreshold)"
                $QUERY_REORGINDEX += " REORGANIZE"
            }
            else {
                $QUERY_REORGINDEX += " REBUILD"
                $rebuild = $true
            }
            if ($index.partition_count -gt 1) {
                $QUERY_REORGINDEX += " PARTITION = $($index.partition_number)"
            }
            if ($DoNotRebuildOnline -eq $false -and $rebuild -eq $true) {
                if ($index.index_type -eq 1) {
                    if ($index.ci_rebuilt_online -eq 1) {
                        $QUERY_REORGINDEX += " WITH (ONLINE = ON)"
                    }
                }
                elseif ($index.index_type -eq 2) {
                    if ($index.nci_rebuilt_online -eq 1) {
                        $QUERY_REORGINDEX += " WITH (ONLINE = ON)"
                    }
                }
            }

            WriteLog -Level CODE -Message $QUERY_REORGINDEX
            ExecuteSqlQuery -connectionString $connectionString -query $QUERY_REORGINDEX
        }
    }
}

function StatsMaintenance {
    Param(
        [string]$connectionString,
        [double]$modctr = 0.2,
        [switch]$donotuse2371
    )

    $stats = GetStatistics -connectionString $connectionString

    foreach ($stat in $stats) {
        $QUERY_UPDATESTATS = "UPDATE STATISTICS [$($stat.schema_name)].[$($stat.object_name)] [$($stat.stats_name)] WITH SAMPLE $SamplePercent PERCENT;"

        if ($stat.stats_rows -eq -1 -and $stat.row_count -gt 0) {
            WriteLog -Level CODE -Message $QUERY_UPDATESTATS
            ExecuteSqlQuery -connectionString $connectionString -query $QUERY_UPDATESTATS
        }
        elseif ($stat.stats_rows -ge 500 -and $stat.stats_modification_counter -gt 0 ) {
            [int]$update_threshold = 500 + ($stat.stats_rows * $modctr)
            [int]$update_thresholdsqrt = [int][System.Math]::Sqrt(1000 * $stat.stats_rows)
            if ($donotuse2371) { $update_thresholdsqrt = $update_threshold}

            if ($stat.stats_modification_counter -gt $update_threshold -or $stat.stats_modification_counter -gt $update_thresholdsqrt) {
                WriteLog -Level CODE -Message $QUERY_UPDATESTATS
                ExecuteSqlQuery -connectionString $connectionString -query $QUERY_UPDATESTATS
            }
        }
    }
}

function ConsistencyCheck {
    Param(
        [string]$connectionString,
        $database
    )

    [string]$DBCC_CHECKDB = "DBCC CHECKDB ([$($database.database_name)]) WITH NO_INFOMSGS, PHYSICAL_ONLY;"
    WriteLog -Level CODE -Message $DBCC_CHECKDB
    try {
        ExecuteSqlQuery -connectionString $connectionString -query $DBCC_CHECKDB
    }
    catch {
        WriteLog -Level ERRO -Message $ERROR_DATABASE_CHECKDB
        WriteLog -Level DEBG -Message $_.Exception
    }
}

function BackupDatabase {
    Param(
        [string]$connectionString,
        $database
    )

    $folder = ""
    if($BackupPath -ne "") {
        $folder = ("$($BackupPath)\$($database.path_name)\$($database.database_name)\").ToLower()
        if (!(Test-Path -Path $folder)) { $null = New-Item -Path $folder -ItemType Directory}
        if (!(Test-Path -Path $folder)) { WriteLog -Level WARN -Message "There was an error creating the backuppath. Backups might fail."}
    }

    $filename = ("$($folder)$($database.database_name)_$(Get-Date -Format yyyyMMddHHmmss)").ToLower()
    [string]$BACKUP_DATABASEFULL = "BACKUP DATABASE [$($database.database_name)] TO DISK = '$($filename)_FULL.bak'"
    [string]$BACKUP_DATABASELOG = "BACKUP LOG [$($database.database_name)] TO DISK = '$($filename)_LOG.bak'"
    [string]$BACKUP_DATABASEDIFF = "BACKUP DATABASE [$($database.database_name)] TO DISK = '$($filename)_DIFF.bak' WITH DIFFERENTIAL"

    WriteLog -Level DEBG -Message "The current backuptype for database $($database.database_name) is $($BackupType) and the recoverymodel is $($database.recovery_model)."
    WriteLog -Level INFO -Message "The current synchronization health for database $($database.database_name) is $($database.synchronization_health)."

    [string]$type = $BackupType
    [bool]$isPreferredBackup = $database.preferred_replica

    if ($database.last_log_backup_lsn.ToString() -eq "" -and $database.recovery_model -ne 3 -and $database.is_read_only -eq 0) {
        $type = "Full"
        ## A full backup is not present. To circumvent this we will create a full backup but only on the primary
        ## The backup preference is ignored
        WriteLog -Level DEBG -Message "Database $($database.database_name) has no valid log backup yet. A full backup will be created first."
        if ($database.is_primary ) { $isPreferredBackup = $true} else {$isPreferredBackup = $false}
    }
    [string]$sql = $BACKUP_DATABASEFULL
    switch ($type) {
        "Full" { 
            if ($isPreferredBackup) {
                if (!$database.is_primary) { 
                    $sql += " WITH COPY_ONLY"
                }
            }
            else {
                WriteLog -Level INFO -Message "Skipping database $($database.database_name) because it is not the preferred backup replica."
            }
        }
        "Differential" {
            if ($database.is_primary -and $database.database_id -gt 1) {
                $sql = $BACKUP_DATABASEDIFF
            }
        }
        "Log" {
            if ($database.recovery_model -ne 3) {
                if($database.is_read_only -eq 1) {
                    WriteLog -Level INFO -Message "Skipping Log backup for database $($database.database_name) because it is read-only."
                    $isPreferredBackup = $false
                }
                if ($isPreferredBackup) {
                    $sql = $BACKUP_DATABASELOG
                }
            }
            else {
                # Set preferred backup to false. This will skip the backup when the recovery model is SIMPLE
                $isPreferredBackup = $false
            }
        }
    }

    try {
        if ($isPreferredBackup) {
            WriteLog -Level CODE -Message $sql
            ExecuteSqlQuery -connectionString $connectionString -query $sql
        }
    }
    catch {
        WriteLog -Level ERRO -Message $ERROR_DATABASE_BACKUP
        WriteLog -Level DEBG -Message $_.Exception.Message
    }
}


## Main Execution
[string]$RunUid = [guid]::NewGuid().ToString()
if($LogPath -eq "") {
    $LogPath = $PSScriptRoot
} else {
    if(!(Test-Path $LogPath)) {
        Write-Host "The logpath is invalid. Logs are written to the current path."
        $LogPath = $PSScriptRoot
    }
}
[string]$LogFile = "$($LogPath)\sqlmaint_$($SqlServer.Replace('\','_'))_$((Get-Date).ToUniversalTime().ToString("yyyyMMdd")).log"
[int]$ErrorCount = 0;

WriteLog -Level INFO -Message "Starting SQL Maintenance ON $($SqlServer)"
WriteLog -Level INFO -Message "Index Maintenance is set to: $($RebuildIndexes)"
WriteLog -Level INFO -Message "Statistics Maintenance is set to: $($UpdateStatistics)"
WriteLog -Level INFO -Message "Database consistency check is set to $($CheckDatabases)"
WriteLog -Level INFO -Message "Database backup is set to $($BackupDatabases)"
WriteLog -Level INFO -Message "Database backup path is set to $($BackupPath)"
WriteLog -Level INFO -Message "The current directory is $PSScriptRoot"
WriteLog -Level INFO -Message "The current logfile is $LogFile"

if ($LowWaterMark -gt $HighWaterMark) {
    WriteLog -Level INFO -Message "The low watermark is set higher then the high watermark. The default values will be used."
    $LowWaterMark = 10
    $HighWaterMark = 30
}

$CONNECTION = "Data Source=$($SqlServer);Initial Catalog=master;Integrated Security=SSPI;Connection Timeout=5;App=SqlMaintenance"
if ($SqlCredential -ne $null) {
    $CONNECTION = "Data Source=$($SqlServer);Initial Catalog=master;Connection Timeout=5;App=SqlMaintenance"
}
$serverDetails = GetServerDetails -connectionString $CONNECTION
WriteLog -Level DEBG -Message "Version is $($serverDetails.ProductVersion) $($serverDetails.Edition)"
if ($serverDetails.ProductVersion -eq $null) {
    WriteLog -Level FATL -Message $ERROR_SERVER_INFORMATION
    $Host.SetShouldExit(1)
    Exit
}
[int]$majorversion = ($serverDetails.ProductVersion.Split("."))[0]
[bool]$isAzure = $false
if($majorversion -lt 11) {
    WriteLog -Level FATL -Message "This version of SQL Server is not supported. Exiting."
    $Host.SetShouldExit(3)
    Exit
}

if ($serverDetails.Edition -like "*Azure*") { $isAzure = $true}
if ($Include.Count -gt 0 -and $Exclude.Count -gt 0) {
    WriteLog -Level INFO -Message "The inclusion list is ignored because there is also an exclusion list."
}

$dbs = GetDatabases -connectionString $CONNECTION -system -majorversion $majorversion -Azure $isAzure
foreach ($db in $dbs) {
    [bool]$skip = $false
    if ($Exclude.Count -gt 0) {
        if ($Exclude -Contains $db.database_name -or ($Exclude -Contains 'system' -and $db.database_id -lt 5)) {
            WriteLog -Level INFO -Message "Database $($db.database_name) is skipped because it is in an exclusion list."
            $skip = $true
        }
    }
    else {
        if ($Include.Count -gt 0 -and (!($include -Contains $db.database_name) -and ($include -Contains 'system' -and $db.database_id -gt 4))) {
            WriteLog -Level INFO -Message "Database $($db.database_name) is skipped because it is not in the included list."
            $skip = $true
        }
    }
    if (!$skip) {
        WriteLog -Level INFO -Message "Changing database context to $($db.database_name)."
        WriteLog -Level CODE -Message "USE [$($db.database_name)]"
        $CONNECTION_DB = "Data Source=$($SqlServer);Initial Catalog=$($db.database_name);Connection Timeout=5;Integrated Security=SSPI;App=SqlMaintenance"
        if ($SqlCredential -ne $null) {
            $CONNECTION_DB = "Data Source=$($SqlServer);Initial Catalog=$($db.database_name);App=SqlMaintenance"
        }
        if ($RebuildIndexes -and $db.database_id -gt 4 -and $db.primary_replica -eq $db.local_server_name) {
            if($db.is_read_only -eq 1 ) {
                WriteLog -Level INFO -Message "Skipping index maintenance for database $($db.database_name) because it is read-only."
            } else {
                IndexMaintenance -connectionString $CONNECTION_DB -lowthreshold $LowWaterMark -highthreshold $HighWaterMark 
            }
        }

        if ($UpdateStatistics -and $db.database_id -gt 4 -and $db.primary_replica -eq $db.local_server_name) {
            if($db.is_read_only -eq 1 ) {
                WriteLog -Level INFO -Message "Skipping statistics maintenance for database $($db.database_name) because it is read-only."
            } else {            
            StatsMaintenance -connectionString $CONNECTION_DB
            }
        }

        if ($CheckDatabases -and $db.database_id -ne 2 -and $db.primary_replica -eq $db.local_server_name) {
            if ($db.database_id -eq 1 -and $isAzure) {
                WriteLog -Level INFO -Message "Cannot perform DBCC CHECKDB on master database in Azure."
            }
            if($db.is_read_only -eq 1 ) {
                WriteLog -Level INFO -Message "Skipping consistency checks for database $($db.database_name) because it is read-only."
            } else {
                ConsistencyCheck -connectionString $CONNECTION_DB -database $db
            }
        }

        if ($BackupDatabases -and $db.database_id -ne 2) {
            if ($isAzure) {
                WriteLog -Level INFO -Message "Skipping backup. Database is on Azure and backup is not supported."
            }
            else {
                BackupDatabase -connectionString $CONNECTION -database $db
            }
        }
    }
}
if ($ErrorCount -gt 0) {
    WriteLog -Level INFO -Message "Finished SQL Maintenance with $($ErrorCount) errors."
    $Host.SetShouldExit(2)
    Exit
}
WriteLog -Level INFO -Message "Finished SQL Maintenance with no errors."