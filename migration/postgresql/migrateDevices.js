const client = require("./client");


async function migrateDevices() {

    const queryForMobilizIotDb = `select * from mobiliziotdb.iot.device`;
    const devicesFromMobilizIotDb = await client.mobilizIotDb.query(queryForMobilizIotDb);

    const devicesFromMobilizIotDbRows = devicesFromMobilizIotDb.rows;
    const selectQuery = 'Select * from devices where id = $1';

    const insertQuery = 'Insert into iot.devices (id, model_id, name, serial_number, company_id, attributes, position_type, longitude, latitude, created_by, created_date, modified_by, modified_date) ' +
        'Values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) ';

    await getDevicesByIdAndInsert(devicesFromMobilizIotDbRows, selectQuery, insertQuery);
}

async function insertDeviceResults(insertQuery, deviceRows, devicesFromMobilizIotDbRows, i, attributes, positionType) {
    await client.nextDb.query(insertQuery,
        [
            deviceRows.id,
            devicesFromMobilizIotDbRows[i].model_id,
            devicesFromMobilizIotDbRows[i].name,
            deviceRows.serial_number,
            devicesFromMobilizIotDbRows[i].company_id,
            JSON.stringify(attributes),
            positionType,
            deviceRows.location_lat,
            deviceRows.location_long,
            deviceRows.created_by,
            deviceRows.created_at,
            deviceRows.modified_by,
            deviceRows.updated_at,
        ], (err, res) => {
            if (err)
                console.log(err.stack);
        })
}

async function getDevicesByIdAndInsert(devicesFromMobilizIotDbRows, selectQuery, insertQuery) {
    for (let i = 0; i < devicesFromMobilizIotDbRows.length; i++) {
        const res = await client.platformApiDb.query(selectQuery, [devicesFromMobilizIotDbRows[i].id]);

        const deviceRows = res.rows[0];
        const attributes = devicesFromMobilizIotDbRows[i].attributes;

        delete attributes["location_long"];
        delete attributes["location_lat"];

        const positionType = devicesFromMobilizIotDbRows[i].attributes.positionType.toUpperCase();

        delete attributes["updateTime"];
        delete attributes["positionType"];
        delete attributes["staticPosition"];
        delete attributes["favouriteSensors"];

        await insertDeviceResults(insertQuery, deviceRows, devicesFromMobilizIotDbRows, i, attributes, positionType);
    }
}

module.exports = migrateDevices;