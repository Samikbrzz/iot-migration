const client = require("./migration/postgresql/client");
const {migrateModels} = require("./migration/postgresql/migrateModels");
const {migrateSensorMappings} = require("./migration/postgresql/migrateSensorMappings");
const {migrateDevices} = require("./migration/postgresql/migrateDevices");
const {migrationUISettings} = require("./migration/postgresql/migrateUISettings");
const {migrationCommands} = require("./migration/postgresql/migrateCommands");
const Console = require("console");
const {updateSequences} = require("./migration/postgresql/updateSequences");
const {getSensorValuesFromFolder} = require("./migration/cassandra/migrateSensorValuesFromBackupFile");

async function migrate() {
    try {
        client.platformApiDb.connect();
        client.mobilizIotDb.connect();
        client.nextDb.connect();
        await migrateModels().then(Console.log("Device Model Migration completed."));
        await migrateSensorMappings().then(Console.log("Sensor mappings migration completed"));
        await migrateDevices().then(Console.log("Devices migration completed."));
        await migrationCommands().then(Console.log("Command migration completed."));
        await migrationUISettings().then(Console.log("UI Settings migration completed."));
        await updateSequences().then(Console.log("Updated sequences."));
        client.platformApiDb.end();
        client.mobilizIotDb.end();
        await getSensorValuesFromFolder().then(Console.log("Migration is starting for sensor values."));
    } catch (e) {
        console.log("Error " + e.stack);
        process.exit();
    }
}

migrate();
