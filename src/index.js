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
    let scores = []
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find()
                    .execute((score) => {
                        scores.push(score)
                    });

    session.close()
    return scores
}

// /list with limit
app.get('/list/:limit', async (req, res) => {
    const scores = await limitAllScores(req.params.limit)
    let msg = {count: scores.length, scores: scores}
    res.send(msg)

})

// Showing find() with limit()
const limitAllScores = async (limit) => {
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

// /bestScores endpoint
app.get('/bestScores/:limit?', async (req, res) => {
    let limit = req.params.limit ? req.params.limit : defaultResultLength
    const scores = await getBestScores(limit)
    let msg = {count: scores.length, scores: scores}
    res.send(msg)
})

// showing find() with fields() and sort()
const getBestScores = async (limit) => {
    let scores = []
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find()
                    .fields(['firstName', 'lastName', 'score', 'date', 'course.name as courseName'])
                    .sort(['score asc', 'date desc'])
                    .limit(limit)
                    .execute((score) => {
                        scores.push(score)
                    });
    session.close()
    return scores
}

app.get('/getRoundsUnderPar', async (req, res) => {
    const scores = await getRoundsUnderPar()
    let msg = {count: scores.length, scores: scores}
    res.send(msg)
})

// Showing find() with numeric comparison
const getRoundsUnderPar = async () => {
    let scores = []
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find("score < course.par")
                    .fields(['firstName', 'lastName', 'score', 'date', 'course.name as courseName'])
                    .execute((score) => {
                        scores.push(score)
                    })
    session.close()
    return scores
}

app.get('/getByScore/:score?', async (req, res) => {
    let score = req.params.score ? req.params.score : 36
    const scores = await getByScore(parseInt(score))
    let msg = {count: scores.length, scores: scores}
    res.send(msg)
})

//show find() with bind()
const getByScore = async (score) => {
    let scores = []
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find("score = :score")
                    .bind('score', score)
                    .fields(['concat(firstName, " ", lastName) as golfer', 'score', 'date', 'course.name as courseName'])
                    .sort(['date desc'])
                    .execute((score) => {
                        scores.push(score)
                    });
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
    let scores = []
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find("lower(lastName) like :lastName")
                    .bind('lastName', lastName.toLowerCase() + '%')
                    .sort(['lastName', 'firstName'])
                    .execute((score) => {
                        scores.push(score)
                    });
    session.close()
    return scores
}

app.get('/getAverageScorePerGolfer/:year?', async (req, res) => {
    const scores = await getAverageScorePerGolfer(req.params.year)
    let msg = {count: scores.length, scores: scores}
    res.send(msg)

})

// show find() using logic for year, aggregate data in fields(), and groupBy()
const getAverageScorePerGolfer = async (year) => {
    let scores = []
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find(year ? "year(date) = " + year : 'date is not null')
                    .fields(['lastName', 'firstName', 'round(avg(score), 2) as avg', 'count(score) as numberOfRounds'])
                    .sort(['lastName', 'firstName'])
                    .groupBy(['lastName', 'firstName'])
                    .execute((score) => {
                        scores.push(score)
                    })
    session.close()
    return scores
}

app.get('/getCourseScoringData', async (req, res) => {
    const scores = await getCourseScoringData()
    let msg = {count: scores.length, scores: scores}
    res.send(msg)
})

// show find with groupBy() on child property.
const getCourseScoringData = async () => {
    let scores = []
    const session = await pool.getSession()
    const db = session.getSchema(databaseName)
    const collection = db.getCollection(collectionName)
    await collection.find()
                    .fields([
                                'course.name as courseName',
                                'round(avg(score), 2)  as avg',
                                'cast(min(score) as unsigned) as lowestScore',
                                'cast(max(score) as unsigned) as highestScore',
                                'count(score) as numberOfRounds'
                            ])
                    .groupBy(['course.name'])
                    .sort('course.name desc')
                    .execute((score) => {
                        scores.push(score)
                    })
    session.close()
    return scores
}

app.get('/getHolesInOne', async (req, res) => {
    const aces = await getHolesInOne()
    let msg = {count: aces.length, aces: aces}
    res.send(msg)
})

// use SQL with path operator, JSON_ARRAYAGG(), JSON_OBJECT(), and JSON_TABLE()
const getHolesInOne = async () => {
    let aces = []
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
    await query.execute((ace) => {
        aces.push(ace)
    })
    session.close()
    return aces
}

app.get('/getAggregateCourseScore', async (req, res) => {
    const courses = await getAggregateCourseScore()
    let msg = {courses: courses}
    res.send(msg)

})

// Show SQL with using JSON data in a common table expression
const getAggregateCourseScore = async () => {
    let courses = []
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
    await query.execute((course) => {
        courses.push(course)
    })
    session.close()
    return courses
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
        await collection.modify("_id = :id").set("holeSores", data.holeScores).bind("id", data._id).execute()
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
        await collection.remove("_id = :id").bind("id", id).execute()
    }
    catch (e) {
        success = false
    }
    session.close()
    return success
}
