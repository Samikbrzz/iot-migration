const client = require("./migration/postgresql/client");
const migrateModels = require("./migration/postgresql/migrateModels");
const migrateSensorMappings = require("./migration/postgresql/migrateSensorMappings");
const migrateDevices = require("./migration/postgresql/migrateDevices");
const migrationUISettings = require("./migration/postgresql/migrateUISettings");
const migrationCommands = require("./migration/postgresql/migrateCommands");
const updateSequences = require("./migration/postgresql/updateSequences");
const getSensorValuesFromFolder = require("./migration/cassandra/migrateSensorValuesFromBackupFile");

async function migrate() {
    try {
        console.log("Migration is starting")
        client.platformApiDb.connect();
        client.mobilizIotDb.connect();
        client.nextDb.connect();
        await migrateModels();
        await migrateSensorMappings();
        await migrateDevices();
        await migrationCommands();
        await migrationUISettings();
        await updateSequences();
        client.platformApiDb.end();
        client.mobilizIotDb.end();
        //await getSensorValuesFromFolder();
    } catch (e) {
        console.log("Error " + e.stack);
        process.exit();
    }
}

migrate();
