// .devcontainer/init-mongo.js
db = db.getSiblingDB('pipeline_db');
db.createUser({
  user: 'devuser',
  pwd: 'devpass',
  roles: [
    { role: 'readWrite', db: 'pipeline_db' }
  ]
});
