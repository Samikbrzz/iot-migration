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
        } else if (file.endsWith("_value.csv")) {
            if (file === 'sensor_latest_value.csv') {
                const sensorLatestValueQuery = 'Insert into iot.sensor_latest_values (device_id, sensor_name, data_type, ts, v_boolean, v_long, v_double, v_string) Values %L';
                await readFilesLineByLineForLatestValues(file, sensorLatestValueQuery);
            }
        }
    }
}

function fileStream(file) {
    const f = '/home/sami/Documents/CassandraBackup/' + file;
    const fileStream = fs.createReadStream(f);

    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });
    return rl;
}

async function readFilesLineByLine(file, sensorName, sensorInsertQuery, dataType) {
    const rl = fileStream(file);

    let values = [];
    for await (const line of rl) {
        let data;
        if (dataType === 'string' && line.includes(',"')) {
            let formattedData;
            data = line.split(',"');
            formattedData = data[0].split(',');
            formattedData.push(data[1]);
            formattedData.splice(1, 1, sensorName);
            formattedData[3] = formattedData[3].replace('"', '');
            data = [];
            data.push(formattedData);
            data = data[0];
        } else {
            data = [];
            data = line.split(',');
            data.splice(1, 1, sensorName);
        }
        values.push(data)
    }

    if (values.length > 0) {
        if (values.length > 200000) {
            let index = 0;
            let incrementBy = 100000;
            for (let i = 0; i < Math.round(values.length / 100000); i++) {
                //await client.nextDb.query(format(sensorInsertQuery, values.slice(index, incrementBy)));
                index = incrementBy;
                incrementBy += 100000;
            }
        } else {
            await client.nextDb.query(format(sensorInsertQuery, values));
        }
    }
}

async function readFilesLineByLineForLatestValues(file, sensorLatestValueQuery) {
    const rl = fileStream(file);

    let values = [];
    for await (const line of rl) {
        let data
        if (!line.includes('"')) {
            data = line.split(',');
        } else {
            let formattedData;
            data = line.split(',"');
            formattedData = data[0].split(',');
            formattedData.push(data[1]);
            formattedData[9] = formattedData[9].replace('"', '');
            data = [];
            data.push(formattedData);
            data = data[0];
        }

        if (data[2] === 'FLOAT') {
            data[8] = data[6];
            data.splice(6, 1);
        } else if (data[2] === 'LONG') {
            data[7] = data[8];
            data.splice(8, 1, null);
            data.splice(5, 1);
        } else {
            data.splice(7, 1);
        }
        data.splice(4, 1);
        for (let i = 0; i < data.length; i++) {
            if (data[i] === '') {
                data[i] = null;
            }
        }

        values.push(data);
    }

    if (values.length > 0) {
        await client.nextDb.query(format(sensorLatestValueQuery, values));
    }
}

module.exports = getSensorValuesFromFolder;