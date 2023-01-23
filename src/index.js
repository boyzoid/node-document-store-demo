// importing the dependencies
import express from 'express'
import * as dotenv from 'dotenv'
dotenv.config()

// DB Specific config
import * as mysqlx from '@mysql/xdevapi';


// defining the Express app
const app = express()

// using bodyParser to parse JSON bodies into JS objects
app.use(express.json())

// Database/Schema Name
const databaseName = 'node_demo'
const collectionName = 'scores'
const connectionUrl = `mysqlx://${process.env.DB_USER}:${process.env.DB_PASSWORD}@${process.env.DB_HOST}:${process.env.DB_PORT}/${databaseName}`

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

// starting the server
app.listen(process.env.PORT, () => {
    console.log('listening on port ' + process.env.PORT)
});

// defining the default endpoint
app.get('/', (req, res) => {
    let msg = {message: 'Node Demo main endpoint'}
    res.send(msg)
});

// /list endpoint
app.get('/list/', async (req, res) => {
    const scores = await listAllScores()
    let msg = {count: scores.length, scores: scores}
    res.send(msg)

})
// find() demo
const listAllScores = async () => {
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    let results = await collection.find()
                    .execute();
    let data = results.fetchAll()
    session.close()
    return data
}

// /list with limit
app.get('/list/:limit/:offset?', async (req, res) => {
    const scores = await limitAllScores(req.params.limit, req.params.offset)
    let msg = {count: scores.length, scores: scores}
    res.send(msg)

})

// Showing find() with limit()
const limitAllScores = async (limit, offset) => {
    if(!offset) offset = 0
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    let results = await collection.find()
                    .limit(limit)
                    .offset(offset)
                    .execute()
    let data = results.fetchAll()
    session.close()
    return data
}

// /bestScores endpoint
app.get('/bestScores/:limit?', async (req, res) => {
    let limit = req.params.limit ? req.params.limit : defaultResultLength
    const scores = await getBestScores(limit)
    let msg = {count: scores.length, scores: scores}
    res.send(msg)
})

// showing find() with fields() and sort()
const getBestScores = async (limit) => {
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    let results = await collection.find()
                    .fields([
                        'firstName',
                        'lastName',
                        'score',
                        'date',
                        'course']
                    )
                    .sort(['score asc', 'date desc'])
                    .limit(limit)
                    .execute()
    let data = results.fetchAll()
    session.close()
    return data
}

app.get('/getRoundsUnderPar', async (req, res) => {
    const scores = await getRoundsUnderPar()
    let msg = {count: scores.length, scores: scores}
    res.send(msg)
})

// Showing find() with numeric comparison
const getRoundsUnderPar = async () => {
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    let results = await collection.find("score < course.par")
                    .fields(['firstName',
                        'lastName',
                        'score',
                        'date',
                        'course.name as courseName'])
                    .execute()
    let data = results.fetchAll()
    session.close()
    return data
}

app.get('/getByScore/:score?', async (req, res) => {
    let score = req.params.score ? req.params.score : 36
    const scores = await getByScore(parseInt(score))
    let msg = {count: scores.length, scores: scores}
    res.send(msg)
})

//show find() with bind()
const getByScore = async (score) => {
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    let results = await collection.find("score = :scoreParam")
                    .bind('scoreParam', score)
                    .fields([
                        'concat(firstName, " ", lastName) as golfer',
                        'score', 'date',
                        'course.name as courseName'
                    ])
                    .sort(['date desc'])
                    .execute()
    let scores = results.fetchAll()
    session.close()
    return scores
}

app.get('/getByGolfer/:lastName?', async (req, res) => {
    let str = req.params.lastName || ''
    const scores = await getByGolfer(str)
    let msg = {count: scores.length, scores: scores}
    res.send(msg)
})

// showing find() using 'like'
const getByGolfer = async (lastName) => {
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    let results = await collection.find("lower(lastName) like :lastNameParam")
                    .bind('lastNameParam', lastName.toLowerCase() + '%')
                    .sort(['lastName', 'firstName'])
                    .execute();
    let data = results.fetchAll()
    session.close()
    return data
}

app.get('/getAverageScorePerGolfer/:year?', async (req, res) => {
    const scores = await getAverageScorePerGolfer(req.params.year)
    let msg = {count: scores.length, scores: scores}
    res.send(msg)

})

// show find() using logic for year, aggregate data in fields(), and groupBy()
const getAverageScorePerGolfer = async (year) => {
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    let results = await collection.find(year ? "year(date) = " + year : 'date is not null')
                    .fields(['lastName', 'firstName', 'round(avg(score), 2) as avg', 'count(score) as numberOfRounds'])
                    .sort(['lastName', 'firstName'])
                    .groupBy(['lastName', 'firstName'])
                    .execute()
    let data = results.fetchAll()
    session.close()
    return data
}

app.get('/getCourseScoringData', async (req, res) => {
    const scores = await getCourseScoringData()
    let msg = {count: scores.length, scores: scores}
    res.send(msg)
})

// show find with groupBy() on child property.
const getCourseScoringData = async () => {
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    let results = await collection.find()
                    .fields([
                                'course.name as courseName',
                                'round(avg(score), 2)  as avg',
                                'cast(min(score) as unsigned) as lowestScore',
                                'cast(max(score) as unsigned) as highestScore',
                                'count(score) as numberOfRounds'
                            ])
                    .groupBy(['course.name'])
                    .sort('course.name desc')
                    .execute()
    let data = results.fetchAll()
    session.close()
    return data
}

app.get('/getHolesInOne', async (req, res) => {
    const aces = await getHolesInOne()
    let msg = {count: aces.length, aces: aces}
    res.send(msg)
})

// use SQL with path operator, JSON_ARRAYAGG(), JSON_OBJECT(), and JSON_TABLE()
const getHolesInOne = async () => {
    const session = await pool.getSession()

    const sql = `SELECT JSON_OBJECT(
                                'firstName', doc ->> '$.firstName',
                                'lastName', doc ->> '$.lastName',
                                'date', doc ->> '$.date',
                                'courseName', doc ->> '$.course.name',
                                'holes', JSON_ARRAYAGG(
                                        JSON_OBJECT(
                                                'holeNumber', holeScores.number,
                                                'par', holeScores.par
                                            )
                                    )
                            )
                 FROM scores,
                      JSON_TABLE(
                              doc, '$.holeScores[*]'
                              COLUMNS (
                                  score INT PATH '$.score',
                                  number INt PATH '$.number',
                                  par INT PATH '$.par'
                                  )
                          ) holeScores
                 WHERE holeScores.score = 1
                 GROUP by doc ->> '$.lastName', doc ->> '$.firstName'
                 ORDER by doc ->> '$.date' DESC`

    const query = await session.sql(sql)
    let results = await query.execute()
    let data = results.fetchAll()
    session.close()
    return data
}

app.get('/getAggregateCourseScore', async (req, res) => {
    const courses = await getAggregateCourseScore()
    let msg = {courses: courses}
    res.send(msg)

})

// Show SQL with using JSON data in a common table expression
const getAggregateCourseScore = async () => {
    const session = await pool.getSession()

    const sql = `
        WITH aggScores AS
                 (SELECT doc ->> '$.course.name' course,
                         MIN(score)              minScore,
                         MAX(score)              maxScore,
                         number
                  FROM scores,
                       JSON_TABLE(doc, '$.holeScores[*]'
                                  COLUMNS (score INT PATH '$.score',
                                      number INT PATH '$.number')) AS scores
                  GROUP BY course, number
                  ORDER BY course, number)
        SELECT JSON_OBJECT('courseName', course, 'bestScore', sum(minScore))
        FROM aggScores
        GROUP BY course
        ORDER BY course;`

    const query = await session.sql(sql)
    let results = await query.execute()
    let data = results.fetchAll()
    session.close()
    return data
}

app.post('/score', async function (req, res, response) {
    const success = await addScore(req.body);
    let msg = {success: success}
    res.send(msg)
});

//Add a score to DB
const addScore = async (score) => {
    let success = true;
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    try {
        await collection.add(score).execute()
    }
    catch (e) {
        success = false
    }
    session.close()
    return success
}

app.post('/holeScores', async function (req, res, response) {
    const success = await addHoleScores(req.body);
    let msg = {success: success}
    res.send(msg)
});

//Add a score to DB
const addHoleScores = async (data) => {
    let success = true;
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    try {
        await collection.modify("_id = :idParam")
            .set("holeSores", data.holeScores)
            .bind("idParam", data._id)
            .execute()
    }
    catch (e) {
        success = false
    }
    session.close()
    return success
}

app.get('/removeScore/:id?', async function (req, res, response) {
    let msg = {}
    if (req.params.id) {
        let success = await removeScore(req.params.id)
        msg.success = success
    }
    else {
        msg.error = "Please provide a valid id."
    }

    res.send(msg)
});

//Add a score to DB
const removeScore = async (id) => {
    let success = true;
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    try {
        await collection.remove("_id = :idParam")
            .bind("idParam", id)
            .execute()
    }
    catch (e) {
        success = false
    }
    session.close()
    return success
}
