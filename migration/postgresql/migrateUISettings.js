const client = require("./client");

async function migrationUISettings() {

    const queryForUISettings = `select * from iot.ui_settings`;
    const UISettingsFromMobilizIotDb = await client.mobilizIotDb.query(queryForUISettings);

    const UISettingsFromMobilizIotDbRows = UISettingsFromMobilizIotDb.rows;

    const insertQuery = 'Insert into iot.ui_settings (id, user_id, name, value, modified_by) ' +
        'Values ($1, $2, $3, $4, \'migration\') ';

    await insertUISettings(insertQuery, UISettingsFromMobilizIotDbRows);
}

async function insertUISettings(insertQuery, uiSettingsRows) {
    for (let i = 0; i < uiSettingsRows.length; i++) {
        await client.nextDb.query(insertQuery,
            [
                uiSettingsRows[i].id,
                uiSettingsRows[i].user_id,
                uiSettingsRows[i].name,
                uiSettingsRows[i].value,
            ], (err, res) => {
                if (err)
                    console.log(err.stack);
            });
    }
}

module.exports = migrationUISettings;