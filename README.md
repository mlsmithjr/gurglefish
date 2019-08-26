
### Gurglefish 
#### Salesforce Archiver

---

Backup your Salesforce sobject data to Postgres and keep in sync.

#### Features

* One-way data snapshots from Salesforce to Postgres.
* Simple CLI interface.
* Dynamic creation of equivalent database table for your selected sobjects.
* Multiprocessing-enabled for up to 4 concurrent table snapshots.
* Automatic creation and maintenance of indexes:
    * Primary key index on ID column
    * Master/Detail and Lookup field IDs.
    * ExternalId fields.
* Automatic detection of sobject field additions/removals and alteration of table structure to match.
* Cloud-ready for Amazon RDS and Azure.
* Synchronization of record additions/changes/deletions since last run.
* Scrubbing of hard deleted records can be disabled on a per-table basis and on the commandline.
* Logging of sync statistics for each table.
* Export feature to enable faster initial data loading using native Postgres load file format.
* Fast field mapping using code generation.
* Schema artifacts saved in a format easily consumed by custom tooling.
* Tested over 18 months continuous running in a production environment.

#### Installation

**Requirements**:
* Python 3.6 or higher
* Postgresql 9.6 or higher

```bash
pip3 install --user gurglefish
```

To update:

```bash
pip3 install --upgrade gurglefish
```

In order to use the _export_ feature to initially populate very large tables (recommended) you must
install the Postgresql CLI client for your system.

Examples:

> Ubuntu 18: ```sudo apt install postgres-client```

> CentOS 7: ```sudo apt install postgres96-client```

Use your distribution package search tool (yum, apt, dnf) to find the correct name and install.

#### Configuration

> **NOTE:** This tool only reads from Salesforce and writes to the database/schema you have configured. However, it is
still very important to security your accounts appropriately to protect against system invasion. In other words,
**security is your responsibility**. 

**Requirements**:

* An API-enabled Salesforce user account with *read-only* access to all sobjects to sync
* A configured Connected App in your Salesforce org accessible by the user account
* A PostgreSQL database and user login with appropriate permission to create and alter tables and indexes only in a schema of your choice.

Create a directory called _/var/lib/gurglefish_.  This is the root of all host storage used by the tool.
Make sure permissions are set accordingly so the running user has r/w privileges. **This directory tree must be protected
appropriately as it may contain unencrypted table exports**.

```bash
sudo mkdir /var/lib/gurglefish
sudo chmod +rwx /var/lib/gurglefish  # set permissions according to your security needs
```

If you want to use a different directory, or a mount point, just create a symbolic link to your location.
Example: ```sudo ln -s /mnt/my-other-storage/sfarchives /var/lib/gurglefish```

Now create the configuration file that provides login credentials to both your Postgres database and Salesforce organization. It is a standard INI file and can contain definitions for multiple database-org relationships.
It will look something like this (for a single database and org):

```ini
[prod]
id=prod
login=my-api-user@myorg.com
password=password+securitytoken
consumer_key=key from connected app
consumer_secret=secret from connected app
authurl=https://my.domain.salesforce.com

dbvendor=postgresql
dbname=gurglefish
schema=public
dbuser=dbadmin
dbpass=dbadmin123
dbhost=192.168.2.61
dbport=5432

threads=2
```
> **NOTE**: Protect this file from prying eyes. You obviously don't want these credentials stolen.

The settings are mostly self explanatory:

* Make sure to include your _password_ **and** security token if you have not whitelisted your IP with Salesforce,
otherwise the token can be omitted.
* The _authurl_ selects either your Salesforce production URL or _https://test.salesforce.com_ for sandboxes.
* Currently, the only supported _dbvendor_ is postgresql.
* The _schema_ can be custom, or *public* (the default). If this database. If the database is to be shared with other critical data it is highly recommended to isolate in a custom schema (see postgresql docs).
* Use _threads_ with caution.  You can have at most 4, as this is a Salesforce-imposed limitation. But the real bottleneck could be your database server.  Without custom database tuning, or running on a small platform, you should stick with 1 or 2 threads.  Move up to 4 only when you are certain the database isn't a bottleneck.

#### Getting Started

Now that you are (hopefully) configured correctly you can pull down a list of sobjects and decide which ones to sync.

Using the example configuration above:
```bash
gurglefish prod --init
```

If the Salesforce login was successful you will see the new directory _/var/lib/gurglefish/db/prod_. This is the root where all configuration and export data will be stored for this connection.

Under _db/prod_ you should see **config.json**.  Open it in your favorite editor.  You will see entries for all sobjects your user account has access to here.

> Note: if you do not see sobjects you know exist, your probably don't have permissions to access them or you need a specific license assigned to your account (in the case of commercial managed packages).

You are free to edit this file as you see fit but make sure it remains valid JSON.  When a new sobject is detected a new entry will be added to this file for it.

Example:

```json
{
    "configuration": {
        "sobjects": [
            {
                "name": "account",
                "enabled": false,
                "auto_scrub": "always"
            },
            {
                "name": "account_vetting__c",
                "enabled": false
            },
            {
                "name": "account_addresses__c",
                "enabled": false
            }
          ]
     }
 }
```
For each sobject you want to sync, set the "enabled" value to **true**.  
For each sobject you want to auto detect and cleanup of deleted records, set "auto_scrub" to "always". But this comes at a cost of API calls and slows down the overall syncing process.  
Alternately, you can schedule a run once a day, or some other interval, to perform the scrub.  Late a night is a good choice.

Sample crontab (global scrub once a day at 1am):

```crontab
0 9,13,15,17,19 * * 1-5	cd /home/masmith/sfarchive && python3 main.py prod --sync >/tmp/sync.log
0 1 * * 1-5	cd /home/masmith/sfarchive && python3 main.py prod --sync --scrub >/tmp/sync.log
```


Save the file. 

You are ready to start pulling data.  But some choices need to be made first.

#### The Initial Data Pull

> This is a good time to discuss the topic of Saleforce API limits.  For each run, for each table, one metadata API call is made to detect schema changes and one query is issues to pull down changed records, giving a minimum of 2 per table per run.  If you run snapshots every 2 hours on 20 tables, that's 12 x 20 x 2 = 480/day **minimum**. I say _minimum_ because this is best-case scenario where there are less than a few hundred changes. With larger data queries, Salesforce returns the data in "chunks" and API users are required to call back to Salesforce to retrieve the next chunk, until are all retrieved.  So for sobjects with a lot of activity, like User, Account, Lead, Opportunity, etc, there could be hundreds of calls for each run.

> Fortunately, Gurglefish reports to you at the end of a run the total number of API calls consumed so you can keep an eye on it. You can compare to the documented limits [here](https://developer.salesforce.com/docs/atlas.en-us.salesforce_app_limits_cheatsheet.meta/salesforce_app_limits_cheatsheet/salesforce_app_limits_platform_api.htm). So, for example, if you have an Enterprise license and 65+ users you already have the maximum of 1,000,000 calls per day limit.

> And remember, you are sharing those limits with your other integrations.


The initial load of data could take quite a bit of time depending on the number of sobjects enabled, number of fields, and number of records. But as a frame of reference, I've seen it process about 200 records per second on an Opportunity table that has over 800 fields.

The recommendation is to just enable a couple of sobjects to start, give it a run to make sure all is going well.  You can then go back and enable other sobjects as you need.  In other words, split up your work. You will consume the most API calls during initial loads so space it out if needed.

**Use standard snapshots**
For any new table load you can stick with standard synchronization/snapshots. Gurglefish will see you are syncing a new table and pull down all records.  Once the initial load is finished, subsequent runs will only pull down the changes.
Snapshot can be interrupted - they will resume where they left off on the next run.

**Use native exports**
Another option is to use the _--export_ feature to dump all sobject records into a postgres native loadable format.  This file can then be loaded using _--load_, usually in under a minute.  Exported files are saved under the __exports/__ folder and compressed.

> NOTE: Exported files are not useful for archiving or backups as their formats are integrally tied to the current schema of their sobject/table.  If that schema changes the exports are not usable. This is a postgres restriction and is the tradeoff for lightning fast loads. You can remove these files after loading.

**Use the Salesforce bulk API**
_This is intended as a last-resort edge case_.
Gurglefish will detect if the SOQL required to retrieve data is longer than 16k and inform you to switch to the bulk API to handle it. Honestly, if you have a table that wide you should rethink your design.
To enable just add "bulkapi":true to the sobject in config.json.  All sync requires going forward will use the Salesforce Bulk API, which in some cases is slower if you have lots of scheduled bulk jobs pending.  Gurglefish will wait up to 10 minutes for the job to start, then time out if it doesn't.

#### Running

```bash
	gurglefish prod --sync
```

Seriously, could it be any easier?

Gurglefish will automatically create any missing tables and indexes in postgres you elected to sync from Salesforce.

#### Snapshot Frequency

It is up to you if you want to schedule automatic runs via **cron** or other mechanism.  Currently, all tables will snapshot on each run - there are not individually customizable run schedules by table. However, this feature is on the roadmap.

#### Statistics

Gurglefish logs statistics for each table on each run to 2 tables:  _gf_mdata_sync_jobs_ (master) and _gf_mdata_sync_stats_ (detail) You are free to query these as you like for reporting, auditing, etc. Job statitics are kept for 2 months and cleaned out as they expire.  So if you want to keep them around longer you should make provisions to sync them elsewhere. A custom trigger to replicate inserts to a longer-term set of tables is a good idea.

Also, a record of automatic schema changes is recorded in _gf_mdata_schema_chg_. So whenever a new or dropped column is detected on an sobject it is recorded here.  This table is never purged.

