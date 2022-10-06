const client = require("./client");

async function migrateModels() {
    const query = deviceModelSelectQuery();

    const modelRes = await client.platformApiDb.query(query);
    const modelResRows = modelRes.rows;

    const insertQuery = 'Insert into iot.device_models (id, company_id, name, notes, protocol_id, attributes, predefined, created_by, created_date, modified_by, modified_date) ' +
        'Values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ';

    await insertDeviceModelResults(modelResRows, insertQuery);
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

async function insertDeviceModelResults(modelResRows, insertQuery) {
    for (let i = 0; i < modelResRows.length; i++) {
        await client.nextDb.query(insertQuery,
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

module.exports = {migrateModels};