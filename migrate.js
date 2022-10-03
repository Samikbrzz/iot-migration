const {config} = require('@mobiliz/utils');
const {Client} = require('pg');
const Console = require("console");
//const sql = require('mssql');

const platformApiDb = new Client(config.platformApiDb);
const mobilizIotDb = new Client(config.mobilizIotDb);
const nextDb = new Client(config.nextDb);

async function migrate() {
    try {
        platformApiDb.connect();
        mobilizIotDb.connect();
        nextDb.connect();
        //await sql.connect(config.ngDb);
        //await migrateModels();
        //await migrateSensorMappings();
        await migrateDevices();
    } catch (e) {
        console.log("Error " + e.stack);
    } 
}

async function migrateModels() {
    const query = deviceModelSelectQuery();

    const modelRes = await platformApiDb.query(query);
    const modelResRows = modelRes.rows;

    const insertQuery = deviceModelInsertQuery();

    await insertDeviceModelResults(modelResRows, insertQuery);
}

function sensorMappingInsertQuery() {
    return 'Insert into iot.sensor_mappings (id, model_id, mapping_type, element_name, sensor_name, data_type, true_label, false_label, formula, map,  created_by, created_date, modified_by, modified_date) ' +
        'Values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) ';
}

async function migrateSensorMappings() {

    const queryForScalar = sensorMappingQueryForScala();
    const queryForFormula = sensorMappingQueryForFormula();
    const queryForDigital = sensorMappingQueryForDigital();
    const queryForTabular = sensorMappingQueryForTabular();

    const sql = queryForScalar + queryForFormula + queryForDigital + queryForTabular;
    const sensorMappings = await platformApiDb.query(sql);
    const sensorMappingsRows = sensorMappings.rows;

    const insertQuery = sensorMappingInsertQuery();

    await insertSensorMappingResults(sensorMappingsRows, insertQuery);
}

async function getDevicesByIdAndInsert(devicesFromMobilizIotDbRows, selectQuery, insertQuery) {
    for (let i = 0; i < devicesFromMobilizIotDbRows.length; i++) {
        await platformApiDb.query(selectQuery,
            [
                devicesFromMobilizIotDbRows[i].id
            ], async (err, res) => {
                if (err) {
                    console.log(err.stack);
                } else {
                    const deviceRows = res.rows[0];
                    const attributes = devicesFromMobilizIotDbRows[i].attributes;

                    if (!deviceRows.is_static_device) {
                        delete attributes["location_long"];
                        delete attributes["location_lat"];
                    }

                    const positionType = devicesFromMobilizIotDbRows[i].attributes.positionType;

                    delete attributes["updateTime"];
                    delete attributes["position_type"];
                    delete attributes["staticPosition"];

                    await insertDeviceResults(insertQuery, deviceRows, devicesFromMobilizIotDbRows, i, attributes, positionType);
                }

            })
    }
}

function deviceModelSelectQuery() {
    return `
       select m.id, -1 as company_id, m."name", '' as notes, dp.protocol_id,
       ('[{"name": "imei", "dataType": "STRING", "required": ' || m.is_imei_required  || '}, 
       {"name": "msisdn", "dataType": "STRING", "required": '|| is_msisdn_required  ||'}]')::jsonb as "attributes",
       'true' as predefined, m.created_by, m.created_at as created_date, 'migration' as modified_by, m.updated_at as modified_date
       from device_models m join device_ports dp on dp.model_id = m.id and dp.channel != 'SMS'
    `;
}

function deviceModelInsertQuery() {
    return 'Insert into iot.device_models (id, company_id, name, notes, protocol_id, attributes, predefined, created_by, created_date, modified_by, modified_date) ' +
        'Values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ';
}

function devicesInsertQuery() {
    return 'Insert into iot.devices (id, model_id, name, company_id, attributes, position_type, longitude, latitude, created_by, created_date, modified_by, modified_date) ' +
        'Values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ';
}

async function migrateDevices() {

    const queryForMobilizIotDb = `select * from mobiliziotdb.iot.device`;
    const devicesFromMobilizIotDb = await mobilizIotDb.query(queryForMobilizIotDb);

    const devicesFromMobilizIotDbRows = devicesFromMobilizIotDb.rows;
    const selectQuery = 'Select * from devices where id = $1';

    const insertQuery = devicesInsertQuery();

    await getDevicesByIdAndInsert(devicesFromMobilizIotDbRows, selectQuery, insertQuery);
}

function sensorMappingQueryForScala() {
    return `
        select ds.id, ds.model_id, 'scalar' as mapping_type, ds."element" as element_name, s.name as sensor_name, s.primitive as data_type,
        null as true_label, null as false_label, null as formula, null::jsonb as "map",
        ds.created_by , ds.created_at as created_date, 'migration' as modified_by, ds.updated_at as modified_date
        from device_sensors_bak ds join sensors s on s.id = ds.sensor_id where ds.model_id is not null and s.protocol_type = 'ANALOG' and s.value_type = 'SCALAR' UNION 
    `;
}

function sensorMappingQueryForFormula() {
    return `
        select ds.id, ds.model_id, 'formula' as mapping_type, ds."element" as element_name, s.name as sensor_name, s.primitive as data_type,
        null as true_label, null as false_label, coalesce(ds.value, s.value)  as formula, null::jsonb as "map",
        ds.created_by , ds.created_at as created_date, 'migration' as modified_by, ds.updated_at as modified_date
        from device_sensors_bak ds join sensors s on s.id = ds.sensor_id where ds.model_id is not null and s.value_type = 'FORMULA' UNION 
    `;
}

function sensorMappingQueryForDigital() {
    return `
        select ds.id, ds.model_id, 'digital' as mapping_type, ds."element" as element_name, s.name as sensor_name, 'BOOLEAN' as data_type,
        cast(s."label"::json->abs(coalesce (ds.value,s.value)::int - 1) as text) as true_label, cast(s."label"::json->coalesce (ds.value,s.value)::int as text) as false_label,
        null as formula, null::jsonb as "map", ds.created_by , ds.created_at as created_date, 'migration' as modified_by, ds.updated_at as modified_date
        from device_sensors_bak ds join sensors s on s.id = ds.sensor_id where ds.model_id is not null and s.protocol_type = 'DIGITAL' UNION 
    `;
}

function sensorMappingQueryForTabular() {
    return `
        select ds.id, ds.model_id, 'tabular' as mapping_type, ds."element" as element_name, s.name as sensor_name, s.primitive as data_type,
        null as true_label, null as false_label, null as formula, ds.value::jsonb  as "map",
        ds.created_by , ds.created_at as created_date, 'migration' as modified_by, ds.updated_at as modified_date
        from device_sensors_bak ds join sensors s on s.id = ds.sensor_id where  ds.model_id is not null and s.value_type = 'TABULAR';
    `;
}

async function insertDeviceModelResults(modelResRows, insertQuery) {
    for (let i = 0; i < modelResRows.length; i++) {
        await nextDb.query(insertQuery,
            [
                modelResRows[i].id,
                modelResRows[i].company_id,
                modelResRows[i].name,
                modelResRows[i].notes,
                modelResRows[i].protocol_id,
                JSON.stringify(modelResRows[i].attributes),
                modelResRows[i].predefined,
                modelResRows[i].created_by,
                modelResRows[i].created_date,
                modelResRows[i].modified_by,
                modelResRows[i].modified_date,
            ], (err, res) => {
                if (err)
                    console.log(err.stack);
            })
    }
}

async function insertSensorMappingResults(sensorMappingsRows, insertQuery) {
    for (let i = 0; i < sensorMappingsRows.length; i++) {
        await nextDb.query(insertQuery,
            [
                sensorMappingsRows[i].id,
                sensorMappingsRows[i].model_id,
                sensorMappingsRows[i].mapping_type,
                sensorMappingsRows[i].element_name,
                sensorMappingsRows[i].sensor_name,
                sensorMappingsRows[i].data_type,
                sensorMappingsRows[i].true_label,
                sensorMappingsRows[i].false_label,
                sensorMappingsRows[i].formula,
                sensorMappingsRows[i].map,
                sensorMappingsRows[i].created_by,
                sensorMappingsRows[i].created_date,
                sensorMappingsRows[i].modified_by,
                sensorMappingsRows[i].modified_date,
            ], (err, res) => {
                if (err)
                    console.log(err.stack);
            })
    }
}

async function insertDeviceResults(insertQuery, deviceRows, devicesFromMobilizIotDbRows, i, attributes, positionType) {
    await nextDb.query(insertQuery,
        [
            deviceRows.id,
            devicesFromMobilizIotDbRows[i].model_id,
            devicesFromMobilizIotDbRows[i].name,
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


migrate();
