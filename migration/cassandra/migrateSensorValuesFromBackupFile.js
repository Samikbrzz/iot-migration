const client = require("../postgresql/client");
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const format = require('pg-format');

async function getSensorValuesFromFolder() {
    const pathString = '/home/sami/Documents/CassandraBackup';

    try {
        fs.readdir(pathString, async (err, files) => {
            if (err)
                console.log(err);
            else {
                for (const file of files) {
                    if (path.extname(file) === ".csv")
                        await parseFileName(file)
                }
                console.log("Migrated values.")
                process.exit()
            }
        })
    } catch (e) {
        console.log(e);
    }

}

async function parseFileName(file) {
    if (file.includes("sensor_")) {
        console.log("Processing = " + file + " file.");
        if (file.endsWith("_long.csv")) {
            const sensorName = file.split("sensor_")[1].split("_long")[0];
            const sensorInputQueryForLong = 'Insert into iot.sensor_values (device_id, sensor_name, ts, v_long) Values %L ';
            await readFilesLineByLine(file, sensorName, sensorInputQueryForLong, '');
        } else if (file.endsWith("_string.csv")) {
            const sensorName = file.split("sensor_")[1].split("_string")[0];
            const sensorInputQueryForString = 'Insert into iot.sensor_values (device_id, sensor_name, ts, v_string) Values %L ';
            await readFilesLineByLine(file, sensorName, sensorInputQueryForString, 'string');
        } else if (file.endsWith("_float.csv")) {
            const sensorName = file.split("sensor_")[1].split("_float")[0];
            const sensorInputQueryForFloat = 'Insert into iot.sensor_values (device_id, sensor_name, ts, v_double) Values %L ';
            await readFilesLineByLine(file, sensorName, sensorInputQueryForFloat, '');
        }
    }
}

async function readFilesLineByLine(file, sensorName, sensorInsertQuery, dataType) {
    const f = '/home/sami/Documents/CassandraBackup/' + file;
    const fileStream = fs.createReadStream(f);

    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let values = [];
    for await (const line of rl) {
        let data = [];
        if (dataType === 'string' && line.includes(',"')) {
            let formattedData = [];
            data = line.split(',"');
            formattedData = data[0].split(',');
            formattedData.push(data[1]);
            data = [];
            formattedData.splice(1, 1, sensorName);
            data.push(formattedData);
        } else {
            data = line.split(',');
            data.splice(1, 1, sensorName);
        }
        values.push(data)
    }
    //console.log(Math.round(values.length / 100000))
    if (values.length > 0) {
        if (values.length > 200000) {
            let index = 0;
            let incrementBy= 100000;
            for (let i = 0; i < Math.round(values.length / 100000); i++) {
                await client.nextDb.query(format(sensorInsertQuery, values.slice(index, incrementBy)));
                index = incrementBy;
                incrementBy += 100000;
            }
        } else {
            await client.nextDb.query(format(sensorInsertQuery, values));
        }
    }
}

module.exports = getSensorValuesFromFolder;