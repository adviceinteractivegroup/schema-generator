'use strict';

var MongoClient = require('mongodb').MongoClient;
var fs = require('fs');
var prettyFormat = require('pretty-format');
var config = require('./config');

var connectionString = 'mongodb://';
connectionString += config.mongoHost + ':';
connectionString += config.mongoPort + '/' + config.mongoDB;

var collectionSchema = {};
var dbHandle = null;

MongoClient.connect(connectionString, function connectionHandler(err, db) {
  if(err) {
    console.log(err);
    process.exit(1);
  }

  dbHandle = db;

  var collection = db.collection(config.mongoCollection);

  var cursor = collection.find(config.filter, function cursorHandler(err, cursor) {
    cursor.each(function(err, item) {
      if (err) {
        console.log(err);
        process.exit(1);
      }

      if(item === null) {
        // we've looped through all items
        return outputResults();
      }

      var fields = Object.keys(item);

      fields.forEach(function(field) {
        if (!collectionSchema[field]) {
          collectionSchema[field] = {count: 0};
        }

        collectionSchema[field].count++;

        // now we check the values
        if(!collectionSchema[field].values) {
          collectionSchema[field].values = [];
        }

        if(collectionSchema[field].values !== 'More than 20') {
          // only add the value if it's not already there
          if(collectionSchema[field].values.indexOf(item[field]) === -1 &&
             (!item[field].length || item[field].length < 100)) {
            collectionSchema[field].values.push(item[field]);
          }
        }

        if(collectionSchema[field].values.length > 20) {
          collectionSchema[field].values = 'More than 20';
        }
      });
    });
  });
});

var outputResults = function outputResults() {
  var filePath = config.mongoDB + '-' + config.mongoCollection;
  if(Object.keys(config.filter).length !== 0) {
    filePath += JSON.stringify(config.filter);
  }
  filePath += '.json';
  filePath = filePath.replace(/"/g, '');
  filePath = filePath.replace(/:/g, '-');

  fs.writeFileSync(filePath, prettyFormat(collectionSchema));
  dbHandle.close();
};
