const client = require("./client");
const {migrateModels} = require("./migrateModels");
const {migrateSensorMappings} = require("./migrateSensorMappings");
const {migrateDevices} = require("./migrateDevices");

async function migrate() {
    try {
        client.platformApiDb.connect();
        client.mobilizIotDb.connect();
        client.nextDb.connect();
        await migrateSensorMappings();
        await migrateModels();
        await migrateDevices();
        return "Completed migration";
    } catch (e) {
        console.log("Error " + e.stack);
    }
}


migrate();
