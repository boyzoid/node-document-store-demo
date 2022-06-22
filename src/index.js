// ./src/index.js

// importing the dependencies
const express = require('express')
const bodyParser = require('body-parser')

// defining the Express app
const app = express()

// using bodyParser to parse JSON bodies into JS objects
app.use(bodyParser.json())

const fs = require('fs')

// DB Specific config
const mysqlx = require('@mysql/xdevapi')

// Database/Schema Name
const databaseName = 'node-demo'
const collectionName = 'scores'

// Connection URL
const url = `mysqlx://root@localhost:33060/`
const defaultResultLength = 50

const initDatabase = async () =>{
    try {
        // Establish the server connection
        const session = await getSession()
        //Drop and recreate schema
        session.dropSchema(databaseName)
        session.createSchema(databaseName)
        const db = session.getSchema(databaseName)
        //create collection
        db.createCollection(collectionName)
        const collection = db.getCollection(collectionName)
        //read scores
        const scores = getDemoData()
        //insert scores
        const result = await collection.add( scores ).execute()
        session.close()
        return result.getAffectedItemsCount()

    } catch (err) {
        console.error(err.stack)
        process.exit(1)
    }
}
const listAllScores = async ( limit ) =>{
    let scores = []
    const session = await getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find().limit(limit).execute(function (score) {
        scores.push(score)
    });

    session.close()
    return scores

}
const getBestScores = async ( limit ) =>{
    let scores = []
    const session = await getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find().fields(['firstName', 'lastName', 'score', 'date', 'course.name', 'course.holeGroup']).sort(['score asc', 'date desc']).limit( limit ).execute(function (score) {
        scores.push(score)
    });
    session.close()
    return scores
}
const getByScore = async ( score ) =>{
    let scores = []
    const session = await getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find("score = :score").bind( 'score', score).sort(['date desc']).execute(function (score) {
        scores.push(score)
    });
    session.close()
    return scores
}
const getByGolfer = async ( lastName ) =>{
    let scores = []
    const session = await getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find("lower(lastName) like :lastName").bind('lastName', lastName.toLowerCase()+'%').execute(function (score) {
        scores.push(score)
    });
    session.close()
    return scores
}
const getSession = async () =>{
    const session = await mysqlx.getSession(url)
    return session
}

const getDemoData = () =>{
    let data = JSON.parse(fs.readFileSync('data/scores.json'))
    return data
}

// defining the default endpoint
app.get('/', (req, res) => {
    let msg = {message: 'Node Demo main endpoint'}
    res.send(msg)
});

app.get('/init', async (req, res) =>{
    const result = await initDatabase()
    let msg = {message: 'Data initialized. Number of rows added: ' + result }
    res.send(msg)

})

app.get('/list/:limit?', async (req, res ) =>{
    let limit = req.params.limit ? req.params.limit : defaultResultLength
    const scores = await listAllScores(limit)
    let msg = { count: scores.length, scores: scores }
    res.send( msg )

})

app.get('/bestScores/:limit?', async (req, res) =>{
    let limit = req.params.limit ? req.params.limit : defaultResultLength
    const scores = await getBestScores(limit);
    let msg = { count: scores.length, scores: scores }
    res.send( msg )
})

app.get('/getByScore/:score?', async (req, res) =>{
    let score = req.params.score ? req.params.score : 36
    const scores = await getByScore(parseInt(score));
    let msg = { count: scores.length, scores: scores }
    res.send( msg )
})

app.get('/getByGolfer/:lastName?', async (req, res) =>{
    let str = req.params.lastName ? req.params.lastName : ''
    const scores = await getByGolfer(str);
    let msg = { count: scores.length, scores: scores }
    res.send( msg )
})

// starting the server
app.listen(3001, () => {
    console.log('listening on port 3001');
});