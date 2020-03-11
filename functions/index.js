const admin = require('firebase-admin');
const Buffer = require("safe-buffer").Buffer;
const functions = require('firebase-functions');
const PubSub = require('@google-cloud/pubsub').PubSub;

const TRIGGER = 0.5;
const BDD_ROOT = 'races';
const TOPIC = 'topic-test';
const BDD_VOTERS_FIELD = 'voters';

const pubsub = new PubSub();

admin.initializeApp();

let db = admin.firestore();

function logError(err) {
    console.log(`Encountered error: ${err}.`);
}

function logDocError(docId, err) {
    console.log(`Error getting document for ${docId} => ${err}.`);
}

function fieldExists(field) {
    return (field.exists) ? field : null;
}

function createNewDoc(docId) {
    return db.collection(BDD_ROOT).doc(docId);
}

async function updateRaceField(raceId, payload) {
    const races = db.collection(BDD_ROOT);
    races.doc(raceId).update(payload);
}

async function publishMessage(topicName, payload) {
    const dataBuffer = Buffer.from(JSON.stringify(payload));
    const topic = pubsub.topic(topicName);
    const publisher = topic.publisher();
    publisher.publish(dataBuffer);
}

async function getRace(raceId) {
    const response = db.collection(BDD_ROOT).doc(raceId).get()
    .then(race => fieldExists(race))
    .catch(err => logDocError(raceId, err));
    return response;
}

async function getVoter(raceId, voterId) {
    const response = db.collection(BDD_ROOT).doc(raceId).collection(BDD_VOTERS_FIELD).doc(voterId).get()
    .then(voter => fieldExists(voter))
    .catch(err => logDocError(voterId, err));
    return response;
}

async function updateRegisteredVotersCount(raceId) {
    const race = await getRace(raceId);
    if (race !== null) {
        let payload = { registeredVotersCount: race.data().registeredVotersCount + 1 };
        await updateRaceField(raceId, payload);
    }
}

async function updateVotersDoneCount(raceId, voterId) {
    const race = await getRace(raceId);
    if (race !== null) {
        const voter = await getVoter(raceId, voterId);
        if (voter !== null) {
            if (voter.data().hasVoted) {
                let payload = { votersDoneCount: race.data().votersDoneCount + 1 };
                await updateRaceField(raceId, payload);
            }
        }
    }
}

async function checkRatio(raceId) {
    const race = await getRace(raceId);
    if (race !== null) {
        const trigger = race.data().trigger;
        const votes = race.data().votersDoneCount;
        const voters = race.data().registeredVotersCount;
        const ratio = (voters > 0) ? votes / voters : 0;
        if (ratio > trigger) {
            const payload = { race: raceId, go: true };
            publishMessage(TOPIC, payload);
        }
    }
}

exports.raceExists = functions.https.onRequest(async (req, res) => {
    let race = await getRace(req.query.id)
    res.send((race === null) ? `No such race !` : `Race ${race.id} found.`);
});

exports.createNewRace = functions.https.onRequest(async (req, res) => {
    const raceId = req.query.id;
    let race = await getRace(raceId);
    if (race === null) {
        race = createNewDoc(raceId);
        race.set({
            go: false,
            trigger: TRIGGER,
            votersDoneCount: 0,
            registeredVotersCount: 0
        })
        .then(p => {
            race.onSnapshot(
                callback => checkRatio(raceId),
                err => logError(err)
            );
            return res.send(`Successfully created a new race for ${race.id}.`);
        })
        .catch(err => {
            res.send(`Failed on creating a new race for ${raceId} => ${err}.`);
        });
    } else {
        res.send(`Failed on creating a new race for ${raceId} => race already exists !`);
    }
});

exports.updateTrigger = functions.https.onRequest(async (req, res) => {
    const raceId = req.query.id;
    const trigger = parseInt(req.query.trigger);
    if (trigger >= 0 && trigger < 1) {
        let race = await getRace(raceId);
        if (race !== null) {
            updateRaceField(raceId, {trigger: trigger});
            res.send(`Successfully updated trigger with value : ${trigger}.`);
        } else {
            res.send("Error : no such race.");
        }
    } else {
        res.send(`Error : trigger value must be : 0 <= trigger < 1.`);
    }
});

exports.addVoter = functions.https.onRequest(async (req, res) => {
    const races = db.collection(BDD_ROOT);
    const raceId = req.query.id;
    const voterId = req.query.voterId;
    const firstName = req.query.firstName;
    let race = await getRace(raceId);
    if (race !== null) {
        let voter = races.doc(raceId).collection(BDD_VOTERS_FIELD).doc(voterId);
        voter.set({
            firstName: firstName,
            hasVoted: false
        });
        updateRegisteredVotersCount(raceId)
        .then(val => {
            let observer = voter.onSnapshot(
                callback => updateVotersDoneCount(raceId, voterId),
                err => logError(err)
            );
            return observer;
        })
        .catch(err => logError(err));
        res.send(`Successfully added voter '${voter.id}' in ${race.id}.`);
    } else {
        res.send("Error : no such race.");
    }
});

exports.listRaces = functions.https.onRequest(async (req, res) => {
    var debug = '';
    db.collection(BDD_ROOT).get()
    .then(snapshot => {
        snapshot.forEach((race) => {
            debug += race.id + ' => go = ' + race.data().go + '<br>';
        });
        return res.send(`${debug}`);
    })
    .catch(err => logError(err));
});