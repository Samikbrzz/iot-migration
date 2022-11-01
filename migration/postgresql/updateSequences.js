const client = require("./client");

async function updateSequences() {

    const commandSeqQuery = await client.platformApiDb.query('Select max(id) from commands');
    const deviceSeqQuery = await client.mobilizIotDb.query('Select max(id) from mobiliziotdb.iot.device');
    const deviceModelSeqQuery = await client.platformApiDb.query('Select max(id) from device_models');
    const sensorMappingSeqQuery = await client.platformApiDb.query('Select max(id) from device_sensors_bak');
    const uiSettingsSeqQuery = await client.mobilizIotDb.query('Select max(id) from iot.ui_settings');

    let commandSeqValue = Number(commandSeqQuery.rows[0].max) + 100;
    let deviceSeqValue = Number(deviceSeqQuery.rows[0].max) + 100;
    let deviceModelSeqValue = Number(deviceModelSeqQuery.rows[0].max) + 100;
    let sensorMappingSeqValue = Number(sensorMappingSeqQuery.rows[0].max) + 100;
    let uiSettingsSeqValue = Number(uiSettingsSeqQuery.rows[0].max) + 100;

    await client.nextDb.query('SELECT setval(\'iot.command_id_seq\', $1)', [commandSeqValue]);
    await client.nextDb.query('SELECT setval(\'iot.device_id_seq\', $1)', [deviceSeqValue]);
    await client.nextDb.query('SELECT setval(\'iot.device_model_id_seq\', $1)', [deviceModelSeqValue]);
    await client.nextDb.query('SELECT setval(\'iot.sensor_mapping_id_seq\', $1)', [sensorMappingSeqValue]);
    await client.nextDb.query('SELECT setval(\'iot.ui_setting_id_seq\', $1)', [uiSettingsSeqValue]);
}

module.exports = {updateSequences};