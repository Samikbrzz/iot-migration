const client = require("./client");

async function migrationCommands() {

    const queryForCommands = `select c.id , '-1' as company_id, c."name", ce.payload as template , true as predefined, jsonb_agg(jsonb_build_object('name', cp."name",'dataType', cp."type", 'defaultValue', cp.default_value)) as params,  
        c.created_by , c.created_at , c.updated_at, 'migration' as modified_by  from commands c 
        join command_executions ce on c.id  = ce.command_id
        join command_params cp on cp.execution_id = ce.id 
        where ce.channel in ('TCP','MQTT') group by c.id, ce.payload`
    ;

    const commandsFromMobilizIotDb = await client.platformApiDb.query(queryForCommands);

    const commandsFromMobilizIotDbRows = commandsFromMobilizIotDb.rows;

    const insertQuery = 'Insert into iot.commands (id, company_id, name, parameters, template, predefined, created_by, created_date, modified_by, modified_date) ' +
        'Values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ';

    await insertCommands(insertQuery, commandsFromMobilizIotDbRows);
}

async function insertCommands(insertQuery, commandRows) {
    for (let i = 0; i < commandRows.length; i++) {
        try {
            await client.nextDb.query(insertQuery,
                [
                    commandRows[i].id,
                    commandRows[i].company_id,
                    commandRows[i].name,
                    JSON.stringify(commandRows[i].params),
                    commandRows[i].template,
                    commandRows[i].predefined,
                    commandRows[i].created_by,
                    commandRows[i].created_at,
                    commandRows[i].modified_by,
                    commandRows[i].updated_at,
                ], (err, res) => {
                    if (err)
                        console.log(err.stack);
                })
        } catch (e) {
            console.error(e);
        }
    }

}

module.exports = migrationCommands;