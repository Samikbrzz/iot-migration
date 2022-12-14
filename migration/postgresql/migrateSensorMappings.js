const client = require("./client");

const sensorMappingsQuery = `
select ms.id, ms.model_id, 'scalar' as mapping_type, ms."element" as element_name, s.name as sensor_name, s.primitive as data_type,
        null as true_label, null as false_label, null as formula, null::jsonb as "map",
        ms.created_by , ms.created_at as created_date, 'migration' as modified_by, ms.updated_at as modified_date
        from model_sensors ms  join sensors s on s.id = ms.sensor_id where ms.model_id is not null and s.protocol_type = 'ANALOG' and s.value_type = 'SCALAR'
union         
select ms.id, ms.model_id, 'formula' as mapping_type, ms."element" as element_name, s.name as sensor_name, s.primitive as data_type,
       null as true_label, null as false_label, coalesce(ms.value, s.value)  as formula, null::jsonb as "map",
       ms.created_by , ms.created_at as created_date, 'migration' as modified_by, ms.updated_at as modified_date
       from model_sensors ms join sensors s on s.id = ms.sensor_id where ms.model_id is not null and s.value_type = 'FORMULA'
union
select ms.id, ms.model_id, 'digital' as mapping_type, ms."element" as element_name, s.name as sensor_name, 'BOOLEAN' as data_type,
        cast(s."label"::json->abs(coalesce (ms.value,s.value)::int - 1) as text) as true_label, cast(s."label"::json->coalesce (ms.value,s.value)::int as text) as false_label,
        null as formula, null::jsonb as "map", ms.created_by , ms.created_at as created_date, 'migration' as modified_by, ms.updated_at as modified_date
        from model_sensors ms join sensors s on s.id = ms.sensor_id where ms.model_id is not null and s.protocol_type = 'DIGITAL'
union
select ms.id, ms.model_id, 'tabular' as mapping_type, ms."element" as element_name, s.name as sensor_name, s.primitive as data_type,
        null as true_label, null as false_label, null as formula, ms.value::jsonb  as "map",
        ms.created_by , ms.created_at as created_date, 'migration' as modified_by, ms.updated_at as modified_date
        from model_sensors ms join sensors s on s.id = ms.sensor_id where ms.model_id is not null and s.value_type = 'TABULAR';
`;


async function migrateSensorMappings() {
    const sensorMappings = await client.platformApiDb.query(sensorMappingsQuery);
    const sensorMappingsRows = sensorMappings.rows;

    const insertQuery = 'Insert into iot.sensor_mappings (id, model_id, mapping_type, element_name, sensor_name, data_type, true_label, false_label, formula, map,  created_by, created_date, modified_by, modified_date) ' +
        'Values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) ';

    await insertSensorMappingResults(sensorMappingsRows, insertQuery);
}

async function insertSensorMappingResults(sensorMappingsRows, insertQuery) {
    for (let i = 0; i < sensorMappingsRows.length; i++) {
        let trueLabel = sensorMappingsRows[i].true_label;
        let falseLabel = sensorMappingsRows[i].false_label;
        if(trueLabel !== null && trueLabel !== undefined && trueLabel.includes('"')) {
            trueLabel = trueLabel.replaceAll('"', '');
        }
        if (falseLabel !== null && falseLabel !== undefined && falseLabel.includes('"')) {
            falseLabel = falseLabel.replaceAll('"', '');
        }
        await client.nextDb.query(insertQuery,
            [
                sensorMappingsRows[i].id,
                sensorMappingsRows[i].model_id,
                sensorMappingsRows[i].mapping_type,
                sensorMappingsRows[i].element_name,
                sensorMappingsRows[i].sensor_name,
                sensorMappingsRows[i].data_type,
                trueLabel,
                falseLabel,
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

module.exports = migrateSensorMappings;