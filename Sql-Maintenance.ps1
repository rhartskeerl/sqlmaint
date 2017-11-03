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
    [string]$SqlServer,
    [pscredential]$SqlCredential,
    [switch]$UpdateStatistics,
    [switch]$RebuildIndexes,
    [switch]$DoNotRebuildOnline,
    [int]$LowWaterMark = 10,
    [int]$HighWaterMark = 30,
    [switch]$CheckDatabases,
    [switch]$BackupDatabases,
    [string]$BackupPath,
    [ValidateSet("Full", "Log", "Differential")]
    $BackupType = "Full"
)

#region Internal Functions
function ExecuteSqlQuery {
    Param (
        [string]$connectionString,
        [string]$query
    )

    $connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
    if($SqlCredential -ne $null) {
        #$Login = New-Object System.Management.Automation.PSCredential -ArgumentList $SqlUser, $Password
        $SqlCredential.Password.MakeReadOnly()
        $sqlCred = New-Object System.Data.SqlClient.SqlCredential($SqlCredential.UserName, $SqlCredential.Password)
        $connection.Credential = $sqlCred
    }
    $command = New-Object System.Data.SqlClient.SqlCommand($query, $connection)
    $command.CommandTimeout = 0
    $datatable = New-Object System.Data.DataTable
    $connection.Open()
    $datatable.Load($command.ExecuteReader())
    $connection.Close()
    return $datatable
}

Function Write-Log {
    Param(
    [ValidateSet("INFO","WARN","ERRO", "FATL", "DEBG", "CODE")]
    [string]$Level = "INFO",
    [Parameter(Mandatory=$True)]
    [string]$Message
    )

    $stamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $line = "$Stamp $Level $RUN_UID $Message"
    If($LOG_FILE) {
        Add-Content $LOG_FILE -Value $line
        Write-Verbose $line
    }
    Else {
        switch ($Level) {
            "WARN" { Write-Warning $line}
            "ERRO" { Write-Error $line}
            "FATL" { Write-Error $line}
            "DEBG" {Write-Debug $line}
            "INFO" {Write-Verbose $line}
            "CODE" {Write-Information $line}
        }
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
    [string]$QUERY_DATABASES = "SELECT dbs.database_id, dbs.name as database_name, dbs.compatibility_level, dbs.user_access, dbs.is_read_only, dbs.recovery_model,
dbrs.database_guid, dbrs.last_log_backup_lsn,
ISNULL((SELECT TOP 1 cluster_name FROM sys.dm_hadr_cluster) + '\' + ag.name, @@SERVERNAME) AS path_name, ISNULL(ags.primary_replica, @@SERVERNAME) AS primary_replica, ISNULL(ag.automated_backup_preference, 0) AS automated_backup_preference,
ISNULL((SELECT TOP 1 replica_server_name FROM sys.availability_replicas WHERE replica_server_name != ags.primary_replica AND backup_priority > 0 ORDER BY backup_priority DESC, replica_server_name), @@SERVERNAME) AS preferred_secondary,
ISNULL((SELECT TOP 1 replica_server_name FROM sys.availability_replicas WHERE backup_priority > 0 ORDER BY backup_priority DESC, replica_server_name), @@SERVERNAME) AS preferred_replica,
@@SERVERNAME AS local_server_name, dbrs.*
FROM sys.databases dbs
INNER JOIN sys.database_recovery_status dbrs ON dbs.database_id = dbrs.database_id 
LEFT JOIN sys.dm_hadr_database_replica_states drs ON dbs.group_database_id = drs.group_database_id AND drs.is_local = 1
LEFT JOIN sys.availability_groups ag ON drs.group_id = ag.group_id
LEFT JOIN sys.dm_hadr_availability_group_states ags ON drs.group_id = ags.group_id
WHERE dbs.state = 0;"

    if($azure)
    {
        $QUERY_DATABASES = "SELECT dbs.database_id, dbs.name as database_name, dbs.compatibility_level, dbs.user_access, dbs.is_read_only, dbs.recovery_model,
NEWID() AS database_guid,0 AS last_log_backup_lsn,
@@SERVERNAME AS path_name, @@SERVERNAME AS primary_replica, 0 AS automated_backup_preference,
@@SERVERNAME AS preferred_secondary,
@@SERVERNAME AS preferred_replica,
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
                Write-Log -Level INFO -Message "Not hitting the threshold... $($index.avg_fragmentation_in_percent) $($highthreshold)"
                $QUERY_REORGINDEX += " REORGANIZE"
            }
            else {
                $QUERY_REORGINDEX += " REBUILD"
                $rebuild = $true
            }
            if ($index.partition_count -gt 1) {
                $QUERY_REORGINDEX += " PARTITION = $($index.partition_number)"
            }
            if($DoNotRebuildOnline -eq $false -and $rebuild -eq $true) {
                if ($index.index_type -eq 1)
                {
                    if($index.ci_rebuilt_online -eq 1)
                    {
                        $QUERY_REORGINDEX += " WITH (ONLINE = ON)"
                    }
                }
                elseif ($index.index_type -eq 2) {
                    if($index.nci_rebuilt_online -eq 1)
                    {
                        $QUERY_REORGINDEX += " WITH (ONLINE = ON)"
                    }
                }
            }

            Write-Log -Level CODE -Message $QUERY_REORGINDEX
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
        $QUERY_UPDATESTATS = "UPDATE STATISTICS [$($stat.schema_name)].[$($stat.object_name)] [$($stat.stats_name)] WITH FULLSCAN;"

        if ($stat.stats_rows -eq -1 -and $stat.row_count -gt 0) {
            Write-Log -Level CODE -Message $QUERY_UPDATESTATS
            ExecuteSqlQuery -connectionString $connectionString -query $QUERY_UPDATESTATS
        }
        elseif ($stat.stats_rows -ge 500 -and $stat.stats_modification_counter -gt 0 ) {
            [int]$update_threshold = 500 + ($stat.stats_rows * $modctr)
            [int]$update_thresholdsqrt = [int][System.Math]::Sqrt(1000 * $stat.stats_rows)
            if ($donotuse2371) { $update_thresholdsqrt = $update_threshold}

            if ($stat.stats_modification_counter -gt $update_threshold -or $stat.stats_modification_counter -gt $update_thresholdsqrt) {
                Write-Log -Level CODE -Message $QUERY_UPDATESTATS
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
    Write-Log -Level CODE -Message $DBCC_CHECKDB
    ExecuteSqlQuery -connectionString $connectionString -query $DBCC_CHECKDB
}

function BackupDatabase {
    Param(
        [string]$connectionString,
        $database
    )

    $folder = ("$($BackupPath)\$($database.path_name)\$($database.database_name)\").ToLower()
    if(!(Test-Path -Path $folder)) { $null = New-Item -Path $folder -ItemType Directory}
    $filename = ("$($folder)$($database.database_name)_$(Get-Date -Format yyyyMMddHHmmss)").ToLower()
    [string]$BACKUP_DATABASEFULL = "BACKUP DATABASE [$($database.database_name)] TO DISK = '$($filename)_FULL.bak'"
    [string]$BACKUP_DATABASELOG = "BACKUP LOG [$($database.database_name)] TO DISK = '$($filename)_LOG.bak'"
    [string]$BACKUP_DATABASEDIFF = "BACKUP DATABASE [$($database.database_name)] TO DISK = '$($filename)_DIFF.bak' WITH DIFFERENTIAL"

    Write-Log -Level DEBG -Message "The current backuptype for database $($database.database_name) is $($BackupType) and the recoverymodel is $($database.recovery_model)"

    [bool]$isPreferredBackup = $false
    [bool]$isPrimary = $false

    if($database.primary_replica -eq $database.local_server_name) {$isPrimary = $true}

    if($database.automated_backup_preference -eq 0 -and $isPrimary) {$isPreferredBackup = $true}
    if($database.automated_backup_preference -eq 1 -and $database.preferred_secondary -eq $database.local_server_name) {$isPreferredBackup = $true}
    if($database.automated_backup_preference -eq 2 -and $database.preferred_secondary -eq $database.local_server_name) {$isPreferredBackup = $true}
    if($database.automated_backup_preference -eq 3 -and $database.preferred_replica -eq $database.local_server_name) {$isPreferredBackup = $true}

    $type = $BackupType
    if($database.last_log_backup_lsn.ToString() -eq "" -and $database.recovery_model -ne 3)
    {
        $type = "Full"
        ## A full backup is not present. To circumvent this we will create a full backup but only on the primary
        ## The backup preference is ignored
        Write-Log -Level DEBG -Message "Database $($database.database_name) has no valid log backup yet. A full backup will be created first."
        if($isPrimary ) { $isPreferredBackup = $true} else {$isPreferredBackup = $false}
    }

    switch ($type) {
        "Full"
        { 
            if($isPreferredBackup)
            {
                if(!$isPrimary) { $BACKUP_DATABASEFULL += " WITH COPY_ONLY"}
                Write-Log -Level CODE -Message $BACKUP_DATABASEFULL
                ExecuteSqlQuery -connectionString $connectionString -query $BACKUP_DATABASEFULL
            }
        }
        "Differential"
        {
            if($isPrimary -and $database.database_id -gt 1)
            {
                Write-Log -Level CODE -Message $BACKUP_DATABASEDIFF
                ExecuteSqlQuery -connectionString $connectionString -query $BACKUP_DATABASEDIFF
            }
        }
        "Log"
        {
            if($database.recovery_model -ne 3)
            {

                if($isPreferredBackup)
                {
                    Write-Log -Level CODE -Message $BACKUP_DATABASELOG
                    ExecuteSqlQuery -connectionString $connectionString -query $BACKUP_DATABASELOG
                }
            }
        }
    }
}

## Main Execution
[string]$RUN_UID = [guid]::NewGuid().ToString()
[string]$LOG_FILE = "$($PSScriptRoot)\sqlmaint_$($SqlServer)_$(Get-Date -Format yyyyMMdd).log"

Write-Log -Level INFO -Message "Starting SQL Maintenance ON $($SqlServer)"
Write-Log -Level INFO -Message "Index Maintenance is set to: $($RebuildIndexes)"
Write-Log -Level INFO -Message "Statistics Maintenance is set to: $($UpdateStatistics)"
Write-Log -Level INFO -Message "Database consistency check is set to $($CheckDatabases)"
Write-Log -Level INFO -Message "Database backup is set to $($BackupDatabases)"
Write-Log -Level INFO -Message "Database backup path is set to $($BackupPath)"
Write-Log -Level INFO -Message "The current directory is $PSScriptRoot"

$CONNECTION = "Data Source=$($SqlServer);Initial Catalog=master;Integrated Security=SSPI;App=SqlMaintenance"
if($SqlCredential -ne $null)
{
    $CONNECTION = "Data Source=$($SqlServer);Initial Catalog=master;App=SqlMaintenance"
}
$serverDetails = GetServerDetails -connectionString $CONNECTION
Write-Log -Level DEBG -Message "Version is $($serverDetails.ProductVersion) $($serverDetails.Edition)"
[int]$majorversion = ($serverDetails.ProductVersion.Split("."))[0]
[bool]$isAzure = $false

if($serverDetails.Edition -like "*Azure*") { $isAzure = $true}

$dbs = GetDatabases -connectionString $CONNECTION -system -majorversion $majorversion -Azure $isAzure
foreach ($db in $dbs) {
    $CONNECTION_DB = "Data Source=$($SqlServer);Initial Catalog=$($db.database_name);Integrated Security=SSPI;App=SqlMaintenance"
    if($SqlCredential -ne $null)
    {
        $CONNECTION_DB = "Data Source=$($SqlServer);Initial Catalog=$($db.database_name);App=SqlMaintenance"
    }
    if ($RebuildIndexes -and $db.database_id -gt 4 -and $db.primary_replica -eq $db.local_server_name) {
        IndexMaintenance -connectionString $CONNECTION_DB -lowthreshold $LowWaterMark -highthreshold $HighWaterMark 
    }

    if ($UpdateStatistics -and $db.database_id -gt 4 -and $db.primary_replica -eq $db.local_server_name) {
        StatsMaintenance -connectionString $CONNECTION_DB
    }

    if($CheckDatabases -and $db.database_id -ne 2 -and $db.primary_replica -eq $db.local_server_name) {
        if($db.database_id -eq 1 -and $isAzure)
        {
            Write-Log -Level INFO -Message "Cannot perform DBCC CHECKDB on master database in Azure."
        }
        else
        {
            ConsistencyCheck -connectionString $CONNECTION_DB -database $db
        }
    }

    if($BackupDatabases -and $db.database_id -ne 2) {
        if($isAzure)
        {
            Write-Log -Level INFO -Message "Skipping backup. Database is on Azure and backup is not supported."
        }
        else {
            BackupDatabase -connectionString $CONNECTION -database $db
        }
    }
}
Write-Log -Level INFO -Message "Finished SQL Maintenance"