# Node.js Demo with MySQL Document Store

A demo for using Node.js to access a MySQL Document Store

## Setup

This setup assumes you already have [Node.js](https://nodejs.org/en/download/) and [MySQl Shell](https://dev.mysql.com/downloads/shell/) installed and have access to a MySQL database.

* Open MySQL Shell and connect to your MySQL instance using the follwoing command: `\c {user}:{password}@{host}:33060`

  * Where `{user}` is the username, `{password}` is the password, and `{host}` is the server domain name or IP address of your MySQL instance.
* In MySQL Shell, run the command `session.createSchema('node_demo')` to create the new schema.
* In MySQL Shell, run the follwoing command: `util.importJson( '/absolute/path/to/project/data/scores.json', {schema: 'node_demo', collection: 'scores'})`

  * If the process runs successfully, you will see output simialr to this:
    `Processed 12.65 MB in 17477 documents in 4.7405 sec (3.69K documents/s)  Total successfully imported documents 17477 (3.69K documents/s)`
* In the project root directory, copy the .env_template file to .env and fill in the values for the port Node will listen on and the database information.
* From a command prompt, run `npm install` in the project root directory.
* From a command prompt, run `node src` in the project root directory.'
* In a browser window, or at tool such as Postman, make a `GET` request to [http://localhost:3001/](http://localhost:3001/)

  * You should see the following result: `{"message":"Node Demo main endpoint"}`
