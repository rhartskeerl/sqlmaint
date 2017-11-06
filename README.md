# SqlMaint

``` powershell
Sql-Maintenance
    -SqlServer <string>
    [-SqlCredential <PSCredentail>]
    [-UpdateStatistics]
    [-SamplePercent <int>]
    [-RebuildIndexes]
    [-DoNotRebuildOnline]
    [-LowWaterMark <int>]
    [-HighWaterMark <int>]
    [-CheckDatabases]
    [-BackupDatabases]
    [-BackupPath <string>]
    [-BackupPath Full|Log|Differential]
```

SqlMaint is a PowerShell script that can help you setup common management tasks on SQL Server. It provides a single solution to automate your backups, consistency checks and index and statistics maintenance.

The script takes a single server as parameter but by wrapping the script in another PowerShell script or job it can be run across several servers.

The following tasks can be performed.

* **Backup:** A full, differential or log backup can be taken. You can specify the root folder and type for the backups. Log backups are only taken for databases in full recovery mode and when no full backup is available a full backup will be performed instead of a log backup.
* **Consistency check:** A DBCC CHECKDB is run for every database and informational messages are surpressed.
* **Index maintenance:** Indexes are reorganized or rebuild based on the provided thresholds. The defaults are to do nothing when fragmentation is below 10%, reorganize when fragmentation is between 10% and 30% and rebuild whenever fragmentation is higher than 30%. Rebuild indexes is performed online when possible but this can be overrided.
* **Statistics maintenance:** Statistics are rebuild when there are more then 500 records and no statstics update has run before. After that statistics update will be performed when 20% of the data was modified or by the behaver of traceflag 2371.

## Examples

### Example 1: Create a full backup on all databases on CONTOSO1

``` powershell
Sql-Maintenance -SqlServer CONTOSO1 -BackupDatabases -BackupPath C:\backup -BackupType Full
```

This command connects to server CONTOSO1 using Windows authentication and backups every database. The final path of the master database might look like this: `C:\backup\contoso1\master\master_full_20171103120000.bak`.

### Example 2: Perform consistency checks on all databases on CONTOSO1

``` powershell
Sql-Maintenance -SqlServer CONTOSO1 -CheckDatabases
```

This command does a consistency check on all databases.

### Example 3: Perform index and statistics maintenance on all databases on CONTOSO1

``` powershell
Sql-Maintenance -SqlServer CONTOSO1 -RebuildIndexes -UpdateStatistics
```

This command will rebuild indexes when needed and then update statistics where needed. Update statistics will always run after the index rebuild or reorganize to avoid updating statistics twice.

## Parameters

### `-SqlServer`

Specifies the server to connect to. This can be the name, name\instance or name.fqdn.

### `-SqlCredential`

In case of SQL authentication a username and password must be provided.

### `-UpdateStatistics`

When provided updates statistics when they hit the defined threshold.

### `-SamplePercent`

The sampling rate in percentage to use when updating statistics. The default is 100.

### `-RebuildIndexes`

When provided rebuilds or reorganizes indexes based on the thresholds. The default is to do nothing when fragmentation is below 10%, reorganize when fragmentation is between 10% and 30% and rebuild when fragmentation is higher then 30%. You can control this through the `-LowWaterMark` and `-HighWaterMark` parameters. Index maintenance is done per partition.

### `-DoNotRebuildOnline`

When provided the script will always rebuild an index offline. The default behavior is to rebuild and index online when possible.

### `-LowWaterMark`

Provides the lower threshold for a reorganize. Indexes that have a fragmentation lower then this number are skipped. This number represents a percentage between 0 and 100.

### `-HighWaterMark`

Provides the upper threshold for a reorganize and the starting point for a rebuild. This number represents a percentage between 0 and 100.

### `-CheckDatabases`

When provided runs DBCC CHECKDB on the database. 

### `-BackupDatabases`

When provided backups up databases. The default backuptype is full and can be controlled with the `-BackupType` parameter. Backups are ignored for Azure SQL Database.

### `-BackupPath`

Defines to root path to place the backups. The folder structure is `root\servername[\instancename]\databasename` for regular databases and `root\clustername\availabilitygroupname\databasename` for databases in an availabilitygroup. The filename is `databasename_yyyyMMddHHmmss_backuptype.bak`, for example: `C:\backup\contoso1\master_20171103120000_full.bak`.

The script creates the paths needed for the backup. When you run this script from a remote computer you should specify a file share to make sure the appropiate folders are created.

### `-BackupType`

Defines the backuptype. This can be Full, Log or Differntial. For availability groups the backup preference is followed. On a secondary a COPY_ONLY backup is performed. Differential backups will only be taken on the primary.

When log backups are specified databases that are in simple recoverymodel are skipped and for databases that don't have a prior full backup a full bakcup will be taken instead of a log backup.

## Supported versions

The script has been tested on SQL versions from SQL Server 2012 through SQL Server 2017 on Windows and should work across all editions. The script isn't testen on Windows or Linux containers and also not on Linux in general. As soon as it is this will be listed here. Also the script isn't tested on the new availability group types; External and None.

## Why

There are plenty of methods and scripts widely available to perform common maintenance tasks on SQL Server on prem and in Azure. I needed a solution that didn't have any dependencies and this does the trick for me. I can run the script from any machine and perform maintenance on any SQL Server. The PowerShell script only relies on .NET and PowerShell scripts and no additional libraries have to be loaded. It can be scheduled through the SQL Agent, Task Scheduler, Azure Automation or your favorite scheduling tool.

## Rules of engagement

You can run the script anywhere you like but I cannot take any responsibility for failures that occur from the use of this script. I do run several tests but cannot cover every scenario with my limited imagination. You are welcome to provide your feedback and contribute to make this script better.