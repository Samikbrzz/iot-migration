const {config} = require('@mobiliz/utils');
const {Client} = require('pg');

const platformApiDb = new Client(config.platformApiDb);
const mobilizIotDb = new Client(config.mobilizIotDb);
const nextDb = new Client(config.nextDb);

module.exports = {platformApiDb, mobilizIotDb, nextDb};