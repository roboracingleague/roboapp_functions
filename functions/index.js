const admin = require('firebase-admin');
const Buffer = require("safe-buffer").Buffer;
const functions = require('firebase-functions');
const PubSub = require('@google-cloud/pubsub').PubSub;

const TRIGGER = 0.5;
const BDD_ROOT = 'races';
const TOPIC = 'topic-test';
const BDD_VOTERS_FIELD = 'voters';

const pubsub = new PubSub();

/*
const serviceAccount = require('../ServiceAccountKey.json');

admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
});
/*/
admin.initializeApp();
//*/

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


/*
const races = db.collection(BDD_ROOT);
var raceId = 'race43';
let observer2 = races.doc(raceId).collection(BDD_VOTERS_FIELD).doc('lolo').onSnapshot(docSnapshot => updateVotersCount(raceId), err => {
    console.log(`Encountered error: ${err}`);
});

/*
let observer = races.doc('race43').onSnapshot(docSnapshot => toto(docSnapshot), err => {
    console.log(`Encountered error: ${err}`);
});
function toto(docSnapshot) {
    console.log(`Received doc snapshot: ${docSnapshot}`);
}
*/



/*
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
*/