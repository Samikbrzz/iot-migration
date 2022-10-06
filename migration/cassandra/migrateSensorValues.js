const cassandra = require('cassandra-driver');
const client = require("../postgresql/client");

async function migrateSensorValues() {

    const PlainTextAuthProvider = cassandra.auth.PlainTextAuthProvider;

    const cassandraClient = new cassandra.Client({
        contactPoints: ['192.168.80.246:9042'],
        localDataCenter: 'datacenter1',
        keyspace: 'iot',
        authProvider: new PlainTextAuthProvider('iot_user', '1fa7fce2acb16be0bc570a85'
        )
    });

    const allTablesQuery = 'describe tables';

    const tables = await cassandraClient.execute(allTablesQuery);

    for (let i = 0; i < tables.rows.length; i++) {
        const sensor = tables.rows[i].name;
        if (sensor.includes("sensor_")) {
            console.log("Processing = " + sensor + " table.");
            if (sensor.endsWith("_long")) {
                const sensorName = sensor.split("sensor_")[1].split("_long")[0];
                const sensorInputQueryForLong = 'Insert into iot.sensor_values (device_id, sensor_name, ts, v_long) ' +
                    'Values ($1, $2, $3, $4) ';
                await getDatasByDateAndInsert(sensor, cassandraClient, sensorName, sensorInputQueryForLong);
            } else if (sensor.endsWith("_string")) {
                const sensorName = sensor.split("sensor_")[1].split("_string")[0];
                const sensorInputQueryForString = 'Insert into iot.sensor_values (device_id, sensor_name, ts, v_string) ' +
                    'Values ($1, $2, $3, $4) ';
                await getDatasByDateAndInsert(sensor, cassandraClient, sensorName, sensorInputQueryForString);
            } else if (sensor.endsWith("_float")) {
                const sensorName = sensor.split("sensor_")[1].split("_float")[0];
                const sensorInputQueryForFloat = 'Insert into iot.sensor_values (device_id, sensor_name, ts, v_double) ' +
                    'Values ($1, $2, $3, $4) ';
                await getDatasByDateAndInsert(sensor, cassandraClient, sensorName, sensorInputQueryForFloat);
            }
        }
    }
}

async function getDatasByDateAndInsert(sensor, cassandraClient, sensorName, sensorInsertQuery) {
    const partitionQuery = 'select distinct device_id, day_epoch from ' + sensor;
    const result = await cassandraClient.execute(partitionQuery);
    cassandraClient.timeout = 500;

    for (let i = 0; i < result.rows.length; i++) {
        let deviceId = result.rows[i].device_id.low;
        const dayEpoch = result.rows[i].day_epoch;

        const dataPartitionQuery = 'select * from ' + sensor + ' where device_id = ' + deviceId + ' and day_epoch = ' + dayEpoch + ' allow filtering ';

        await cassandraClient.execute(dataPartitionQuery, [], {fetchSize: 20000, autoPage: true})
            .then(async result => await insertSensorValues(result, sensorInsertQuery, sensorName));
    }
}

async function insertSensorValues(result, sensorInsertQuery, sensorName) {
    for (let i = 0; i < result.rows.length; i++) {
        let value = result.rows[i].value.low;
        if (value === undefined) {
            value = result.rows[i].value;
        }
        await client.nextDb.query(sensorInsertQuery,
            [
                result.rows[i].device_id.low,
                sensorName,
                result.rows[i].event_ts,
                value
            ], (err, res) => {
                if (err) {
                    console.log(err);
                    process.exit();
                }
            })
    }
}

module.exports = {migrateSensorValues};

