const admin = require('firebase-admin');
const Buffer = require("safe-buffer").Buffer;
const functions = require('firebase-functions');
const PubSub = require('@google-cloud/pubsub').PubSub;

const TRIGGER = 0.5;
const BDD_ROOT = '/races';
const BDD_GO_FIELD = 'go';
const TOPIC = 'topic-test';
const BDD_RACE_FIELD = 'race';
const BDD_VOTES_FIELD = 'votes';
const BDD_VOTERS_FIELD = 'voters';

const pubsub = new PubSub();

admin.initializeApp();

function getValueInDatabase(path, field) {
    const fieldInDB = admin.database().ref(path + "/" + field);
    return getValueFromField(fieldInDB);
}

async function setValueInDatabase(path, payload) {
    admin.database().ref(path).set(payload);
}

async function updateValueInDatabase(path, payload) {
    admin.database().ref(path).update(payload);
}

async function deleteFromDB(path) {
    admin.database().ref(path).remove();
}

function getValueFromField(field) {
    var value = 0;
    field.on('value', (snapshot) => { value = snapshot.val(); });
    return value;
}

function checkIfRaceExists(path) {
    const raceField = admin.database().ref(path);
    var exists = false;
    raceField.on('value', (snapshot) => { exists = snapshot.exists(); });
    return exists;
}

async function publishMessage(topicName, payload) {
    const dataBuffer = Buffer.from(JSON.stringify(payload));
    const topic = pubsub.topic(topicName);
    const publisher = topic.publisher();
    publisher.publish(dataBuffer);
}

exports.clearDatabase = functions.https.onRequest(async (req, res) => {
    await deleteFromDB(BDD_ROOT);
    res.send(`Removed all data in database`);
});

exports.createNewRace = functions.https.onRequest(async (req, res) => {
    const raceId = req.query.id;
    const voters = req.query.nbVoters;
    const path = BDD_ROOT + '/' + BDD_RACE_FIELD + raceId;
    if (checkIfRaceExists(path))
        await deleteFromDB(path);
    const payload = { voters: parseInt(voters), votes: 0, go: false };
    await setValueInDatabase(path, payload);
    res.send(`Created a new structure for race ${raceId} and added ${voters} in database.`);
});

exports.deleteRace = functions.https.onRequest(async (req, res) => {
    const raceId = req.query.id;
    const path = BDD_ROOT + '/' + BDD_RACE_FIELD + raceId;
    await deleteFromDB(path);
    res.send(`Removed race ${raceId} in database.`);
});

exports.createNewRaceListener = functions.database.ref(BDD_ROOT + '/{pushId}/' + BDD_VOTERS_FIELD).onCreate( async (snapshot, context) => {
    const race = context.params.pushId;
    const voters = snapshot.val();
    const payload = { race: race, voters: parseInt(voters) };
    await publishMessage(TOPIC, payload);
    return snapshot.ref.parent.child(BDD_VOTES_FIELD).set(0);
});

exports.addVote = functions.https.onRequest(async (req, res) => {
    const raceId = req.query.id;
    try {
        const path = BDD_ROOT + '/' + BDD_RACE_FIELD + raceId;
        if (!checkIfRaceExists(path)) {
            res.send("Race doesn't exist !");
            return;
        }
        const nbrVotes = getValueInDatabase(path, BDD_VOTES_FIELD) + 1;
        const nbrVoters = getValueInDatabase(path, BDD_VOTERS_FIELD);
        const go = getValueInDatabase(path,  BDD_GO_FIELD);

        const ratio = (nbrVoters > 0) ? nbrVotes / nbrVoters : 0;
        var debug = `Added a new vote for race ${raceId}. (nb votes = ${nbrVotes} and ratio is ${ratio})`;
        await updateValueInDatabase(path, {votes: parseInt(nbrVotes)});
        if (ratio > TRIGGER && !go) {
            const payload = { go: true };
            await updateValueInDatabase(path, payload);
            await publishMessage(TOPIC, payload);
            debug += `<br>Send a go !`
        }
        res.send(debug);
    } catch(e) {
        res.send(`Exception catched : ${e})`);
    }
});