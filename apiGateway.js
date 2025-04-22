const express = require('express');
const { ApolloServer } = require('@apollo/server');
const { expressMiddleware } = require('@apollo/server/express4');
const bodyParser = require('body-parser');
const cors = require('cors');

const typeDefs = require('./schema');
const resolvers = require('./resolvers');

const app = express();
app.use(cors());
app.use(bodyParser.json());

const server = new ApolloServer({ typeDefs, resolvers });
server.start().then(() => {
  app.use('/graphql', expressMiddleware(server));
  app.listen(3000, () => console.log("API Gateway running on port 3000"));
});