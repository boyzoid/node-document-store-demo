# Node.js Demo with MySQL Document Store

A demo for using Node.js to access a MySQL Document Store

## Setup

This setup assumes you already have Node.js installed and have access to a MySQL database.

* On your instance of MySQL, run the following command ```CREATE SCHEMA `node-demo` ;```
* Copy the .env_template file to .env and fill in the values for the port Node will listen on and the database information.
* Run `npm install` in the root directory.
* Run `node src` in the root directory.'
* Make a GET call to `localhost:{port number}/init` to load data into the database.
