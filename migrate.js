const client = require("./migration/postgresql/client");
const migrateModels = require("./migration/postgresql/migrateModels");
const migrateSensorMappings = require("./migration/postgresql/migrateSensorMappings");
const migrateDevices = require("./migration/postgresql/migrateDevices");
const migrateSensorValues = require("./migration/cassandra/migrateSensorValues");
const migrationUISettings = require("./migration/postgresql/migrateUISettings");
const migrationCommands = require("./migration/postgresql/migrateCommands");

async function migrate() {
    try {
        client.platformApiDb.connect();
        client.mobilizIotDb.connect();
        client.nextDb.connect();
        await migrateModels().then(console.log("Device Model Migration completed."));
        await migrateSensorMappings().then(console.log("Sensor mappings migration completed"));
        await migrateDevices().then(console.log("Devices migration completed."));
        await migrationCommands().then(console.log("Command migration completed."));
        await migrationUISettings().then(console.log("UI Settings migration completed."));
        await migrateSensorValues().then(console.log("Sensor values migration is starting..."));
    } catch (e) {
        console.log("Error " + e.stack);
        process.exit();
    }
}


migrate();
