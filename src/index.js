// ./src/index.js

// importing the dependencies
const express = require('express')
const bodyParser = require('body-parser')
require('dotenv').config();

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
const connectionUrl =  `mysqlx://${process.env.DB_USER}:${process.env.DB_PASSWORD}@${process.env.DB_HOST}:${process.env.DB_PORT}/${databaseName}`

// Connection URL
const pool = mysqlx.getClient(connectionUrl, {
    pooling: {
        enabled: true,
        maxSize: 10,
        maxIdleTime: 20000,
        queueTimeout: 5000
    }
})
const defaultResultLength = 50

const initDatabase = async () =>{
    try {
        // Establish the server connection
        const session = await pool.getSession()
        // get the schema
        const db = session.getSchema(databaseName)
        // drop collection if it exists
        db.dropCollection(collectionName)
        //create collection
        db.createCollection(collectionName)
        const collection = db.getCollection(collectionName)
        //read scores
        const scores = getDemoData()
        //insert scores
        const result = await collection.add( scores ).execute()
        // Add indexes
        collection.createIndex("courseName", {fields: [{field: "$.course.name", type: "TEXT(100)"}]})
        collection.createIndex("golferLastName", {fields: [{field: "$.lastName", type: "TEXT(100)"}]})
        //close the session
        session.close()
        return result.getAffectedItemsCount()

    } catch (err) {
        console.error(err.stack)
        process.exit(1)
    }
}
const listAllScores = async ( limit ) =>{
    let scores = []
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find()
        .limit(limit)
        .execute((score) => {
            scores.push(score)
        });

    session.close()
    return scores

}
const getBestScores = async ( limit ) =>{
    let scores = []
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find()
        .fields(['firstName', 'lastName', 'score', 'date', 'course.name as courseName'])
        .sort(['score asc', 'date desc']).limit( limit )
        .execute((score) => {
            scores.push(score)
        });
    session.close()
    return scores
}
const getByScore = async ( score ) =>{
    let scores = []
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find("score = :score")
        .bind( 'score', score)
        .sort(['date desc'])
        .execute((score) => {
            scores.push(score)
        });
    session.close()
    return scores
}
const getByGolfer = async ( lastName ) =>{
    let scores = []
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find("lower(lastName) like :lastName")
        .bind('lastName', lastName.toLowerCase()+'%')
        .sort(['lastName', 'firstName'])
        .execute((score) => {
            scores.push(score)
        });
    session.close()
    return scores
}
const getRoundsUnderPar = async () =>{
    let scores = []
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find("score < course.par")
        .fields(['firstName', 'lastName', 'score', 'date', 'course.name as courseName'])
        .execute((score) => {
            scores.push( score )
        })
    session.close()
    return scores
}

const getAverageScorePerGolfer = async () =>{
    let scores = []
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find()
        .fields(['lastName', 'firstName', 'avg(score) as avg', 'count(score) as numberOfRounds'])
        .groupBy(['lastName', 'firstName'])
        .execute((score) => {
            scores.push( score )
        })
    session.close()
    return scores
}

const getCourseScoringData= async () =>{
    let scores = []
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find()
        .fields(['course.name as courseName', 'course.slope as slope', 'course.rating as rating', 'format(avg(score), 2) * 1  as avg', 'min(score) * 1 as lowestScore', 'max(score) * 1 as highestScore',
         'count(score) as numberOfRounds'])
        .groupBy(['course.name'])
        .sort('course.name')
        .execute((score) => {
            scores.push( score )
        })
    session.close()
    return scores
}
const getDetailedScoreInfoPerCourse = async () =>{
    let courses = []
    const session = await pool.getSession()
    const query = await session.sql(
        "with rounds as ( " +
        "select doc->> '$.firstName' as firstName, " +
        "       doc->> '$.lastName' as lastName, " +
        "       doc->> '$.score' * 1 as score, " +
        "       doc->> '$.course.name' as courseName, " +
        "       doc->> '$.date' as datePlayed " +
        "from scores ), " +
        "roundsAgg as ( " +
        "select courseName, min( score ) lowScore from rounds group by courseName " +
        ") " +
        "select  JSON_OBJECT('courseName', ra.courseName, " +
        "                    'score', ra.lowScore, " +
        "                    'golfers', ( " +
        "                        select JSON_ARRAYAGG( " +
        "                            JSON_OBJECT('golfer', concat(r.firstName, ' ', r.lastName), 'datePlayed', r.datePlayed) " +
        "                        ) " +
        "                        from rounds r " +
        "                        where r.score = ra.lowScore " +
        "                        and r.courseName = ra.courseName) " +
        "        ) as data " +
        " " +
        "from roundsAgg ra " +
        "group by ra.courseName " +
        "order by ra.courseName;")
    await query.execute( (course) => {
        courses.push(course)
    })
    session.close()
    return courses
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
app.get('/getRoundsUnderPar', async (req, res) =>{
    const scores = await getRoundsUnderPar();
    let msg = { count: scores.length, scores: scores }
    res.send( msg )
    
})
app.get('/getAverageScorePerGolfer', async (req, res) =>{
    const scores = await getAverageScorePerGolfer();
    let msg = { count: scores.length, scores: scores }
    res.send( msg )
    
})
app.get('/getCourseScoringData', async (req, res) =>{
    const scores = await getCourseScoringData();
    let msg = { count: scores.length, scores: scores }
    res.send( msg )
})

app.get('/getDetailedScoreInfoPerCourse', async (req, res) => {
    const courses = await getDetailedScoreInfoPerCourse();
    let msg = { count: courses.length, courses: courses }
    res.send( msg )
})
// starting the server
app.listen(process.env.PORT, () => {
    console.log('listening on port ' + process.env.PORT)
});
